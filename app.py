import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib import error, request

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

load_dotenv()

BASE_DIR = Path(__file__).resolve().parent
DATASET_DIR = BASE_DIR / "dataset" / "sap-o2c-data"
DATA_DIR = BASE_DIR / "data"
STATIC_DIR = BASE_DIR / "static"
DB_PATH = DATA_DIR / "o2c_graph.db"
BUSINESS_ENTITY_TYPES = {
    "Customer",
    "Address",
    "Sales Order",
    "Product",
    "Plant",
    "Delivery",
    "Billing Document",
    "Journal Entry",
    "Payment",
}
GRANULAR_ENTITY_TYPES = {
    "Customer Company Assignment",
    "Customer Sales Area",
    "Sales Order Item",
    "Schedule Line",
    "Product Master",
    "Product Plant",
    "Storage Location",
    "Delivery Item",
    "Billing Item",
}
ALLOWED_SQL_TABLES = {
    "customers",
    "customer_company_assignments",
    "customer_sales_area_assignments",
    "business_partner_addresses",
    "plants",
    "products",
    "product_master",
    "product_plants",
    "product_storage_locations",
    "sales_orders",
    "sales_order_items",
    "sales_order_schedule_lines",
    "deliveries",
    "delivery_items",
    "billing_documents",
    "billing_document_items",
    "journal_entries",
    "payments",
    "inferred_flow_links",
}


def iso_to_date(value: str | None) -> str | None:
    return value[:10] if value else None


def to_float(value: Any) -> float | None:
    try:
        return float(value) if value not in (None, "") else None
    except (TypeError, ValueError):
        return None


def read_jsonl(folder: str) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in sorted((DATASET_DIR / folder).glob("*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle)
    return rows


class ChatRequest(BaseModel):
    message: str


class O2CRepository:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        if not self.db_path.exists():
            self._build_database()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def _build_database(self) -> None:
        conn = self.connect()
        cur = conn.cursor()
        cur.executescript(
            """
            CREATE TABLE customers (customer_id TEXT PRIMARY KEY, name TEXT, category TEXT, grouping_code TEXT, created_on TEXT, blocked INTEGER);
            CREATE TABLE customer_company_assignments (
                assignment_id TEXT PRIMARY KEY, customer_id TEXT, company_code TEXT, payment_terms TEXT,
                reconciliation_account TEXT, deletion_indicator INTEGER, customer_account_group TEXT
            );
            CREATE TABLE customer_sales_area_assignments (
                assignment_id TEXT PRIMARY KEY, customer_id TEXT, sales_organization TEXT, distribution_channel TEXT,
                division TEXT, currency TEXT, customer_payment_terms TEXT, incoterms TEXT,
                incoterms_location TEXT, shipping_condition TEXT, supplying_plant TEXT
            );
            CREATE TABLE business_partner_addresses (
                address_id TEXT PRIMARY KEY, customer_id TEXT, city_name TEXT, country TEXT,
                postal_code TEXT, region TEXT, street_name TEXT
            );
            CREATE TABLE plants (plant_id TEXT PRIMARY KEY, plant_name TEXT, address_id TEXT, distribution_channel TEXT, division TEXT);
            CREATE TABLE products (product_id TEXT PRIMARY KEY, description TEXT);
            CREATE TABLE product_master (
                product_id TEXT PRIMARY KEY, product_type TEXT, product_group TEXT,
                base_unit TEXT, gross_weight REAL, net_weight REAL, weight_unit TEXT, division TEXT
            );
            CREATE TABLE product_plants (
                product_plant_id TEXT PRIMARY KEY, product_id TEXT, plant_id TEXT,
                availability_check_type TEXT, profit_center TEXT, mrp_type TEXT
            );
            CREATE TABLE product_storage_locations (
                location_id TEXT PRIMARY KEY, product_id TEXT, plant_id TEXT, storage_location TEXT,
                physical_inventory_block TEXT, last_count_date TEXT
            );
            CREATE TABLE sales_orders (
                sales_order_id TEXT PRIMARY KEY, customer_id TEXT, sales_organization TEXT, distribution_channel TEXT, division TEXT,
                creation_date TEXT, requested_delivery_date TEXT, total_net_amount REAL, currency TEXT, overall_delivery_status TEXT,
                billing_status TEXT, incoterms TEXT, incoterms_location TEXT, payment_terms TEXT, primary_plant TEXT
            );
            CREATE TABLE sales_order_items (
                item_id TEXT PRIMARY KEY, sales_order_id TEXT, sales_order_item TEXT, product_id TEXT, requested_quantity REAL,
                quantity_unit TEXT, net_amount REAL, material_group TEXT, production_plant TEXT, storage_location TEXT
            );
            CREATE TABLE sales_order_schedule_lines (
                schedule_line_id TEXT PRIMARY KEY, sales_order_id TEXT, sales_order_item TEXT,
                schedule_line TEXT, confirmed_delivery_date TEXT, order_quantity_unit TEXT, confirmed_quantity REAL
            );
            CREATE TABLE deliveries (delivery_id TEXT PRIMARY KEY, creation_date TEXT, shipping_point TEXT, goods_movement_status TEXT, picking_status TEXT);
            CREATE TABLE delivery_items (
                delivery_item_id TEXT PRIMARY KEY, delivery_id TEXT, delivery_item TEXT, sales_order_id TEXT,
                sales_order_item TEXT, product_id TEXT, plant_id TEXT, storage_location TEXT, delivery_quantity REAL, delivery_quantity_unit TEXT
            );
            CREATE TABLE billing_documents (
                billing_document_id TEXT PRIMARY KEY, customer_id TEXT, company_code TEXT, fiscal_year TEXT, accounting_document_id TEXT,
                billing_date TEXT, total_net_amount REAL, currency TEXT, cancelled INTEGER
            );
            CREATE TABLE billing_document_items (
                billing_item_id TEXT PRIMARY KEY, billing_document_id TEXT, billing_document_item TEXT,
                product_id TEXT, delivery_id TEXT, delivery_item TEXT, billing_quantity REAL, billing_quantity_unit TEXT, net_amount REAL, currency TEXT
            );
            CREATE TABLE journal_entries (
                journal_entry_id TEXT PRIMARY KEY, customer_id TEXT, company_code TEXT, fiscal_year TEXT, billing_document_id TEXT,
                gl_account TEXT, accounting_document_type TEXT, posting_date TEXT, amount REAL, clearing_accounting_document TEXT
            );
            CREATE TABLE payments (
                payment_id TEXT PRIMARY KEY, accounting_document_id TEXT, customer_id TEXT, company_code TEXT, fiscal_year TEXT,
                clearing_accounting_document TEXT, posting_date TEXT, amount REAL, currency TEXT
            );
            CREATE TABLE inferred_flow_links (link_type TEXT, source_id TEXT, target_id TEXT, confidence REAL, reason TEXT);
            CREATE TABLE graph_nodes (node_id TEXT PRIMARY KEY, label TEXT, entity_type TEXT, metadata_json TEXT);
            CREATE TABLE graph_edges (edge_id TEXT PRIMARY KEY, source_id TEXT, target_id TEXT, relation_type TEXT, confidence REAL, metadata_json TEXT);
            """
        )

        customers = read_jsonl("business_partners")
        customer_company_assignments = read_jsonl("customer_company_assignments")
        customer_sales_area_assignments = read_jsonl("customer_sales_area_assignments")
        customer_addresses = read_jsonl("business_partner_addresses")
        plants = read_jsonl("plants")
        product_master_rows = read_jsonl("products")
        product_rows = read_jsonl("product_descriptions")
        product_plant_rows = read_jsonl("product_plants")
        product_storage_locations = read_jsonl("product_storage_locations")
        sales_orders = read_jsonl("sales_order_headers")
        sales_items = read_jsonl("sales_order_items")
        sales_schedule_lines = read_jsonl("sales_order_schedule_lines")
        deliveries = read_jsonl("outbound_delivery_headers")
        delivery_items = read_jsonl("outbound_delivery_items")
        billing_headers = read_jsonl("billing_document_headers")
        billings = read_jsonl("billing_document_cancellations")
        billing_items = read_jsonl("billing_document_items")
        journal_entries = read_jsonl("journal_entry_items_accounts_receivable")
        payments = read_jsonl("payments_accounts_receivable")

        cur.executemany(
            "INSERT INTO customers VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    row["customer"],
                    row.get("businessPartnerFullName") or row.get("businessPartnerName"),
                    row.get("businessPartnerCategory"),
                    row.get("businessPartnerGrouping"),
                    iso_to_date(row.get("creationDate")),
                    1 if row.get("businessPartnerIsBlocked") else 0,
                )
                for row in customers
            ],
        )
        cur.executemany(
            "INSERT INTO customer_company_assignments VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('customer')}:{row.get('companyCode')}",
                    row.get("customer"),
                    row.get("companyCode"),
                    row.get("paymentTerms"),
                    row.get("reconciliationAccount"),
                    1 if row.get("deletionIndicator") else 0,
                    row.get("customerAccountGroup"),
                )
                for row in customer_company_assignments
            ],
        )
        cur.executemany(
            "INSERT INTO customer_sales_area_assignments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('customer')}:{row.get('salesOrganization')}:{row.get('distributionChannel')}:{row.get('division')}",
                    row.get("customer"),
                    row.get("salesOrganization"),
                    row.get("distributionChannel"),
                    row.get("division"),
                    row.get("currency"),
                    row.get("customerPaymentTerms"),
                    row.get("incotermsClassification"),
                    row.get("incotermsLocation1"),
                    row.get("shippingCondition"),
                    row.get("supplyingPlant"),
                )
                for row in customer_sales_area_assignments
            ],
        )
        cur.executemany(
            "INSERT INTO business_partner_addresses VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.get("addressId"),
                    row.get("businessPartner"),
                    row.get("cityName"),
                    row.get("country"),
                    row.get("postalCode"),
                    row.get("region"),
                    row.get("streetName"),
                )
                for row in customer_addresses
            ],
        )
        cur.executemany(
            "INSERT INTO plants VALUES (?, ?, ?, ?, ?)",
            [
                (row.get("plant"), row.get("plantName"), row.get("addressId"), row.get("distributionChannel"), row.get("division"))
                for row in plants
            ],
        )

        product_map: dict[str, str] = {}
        for row in product_rows:
            if row.get("language") == "EN":
                product_map[row["product"]] = row.get("productDescription", "")
        cur.executemany("INSERT INTO products VALUES (?, ?)", [(k, v) for k, v in product_map.items()])
        cur.executemany(
            "INSERT INTO product_master VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    row.get("product"),
                    row.get("productType"),
                    row.get("productGroup"),
                    row.get("baseUnit"),
                    to_float(row.get("grossWeight")),
                    to_float(row.get("netWeight")),
                    row.get("weightUnit"),
                    row.get("division"),
                )
                for row in product_master_rows
            ],
        )
        cur.executemany(
            "INSERT INTO product_plants VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('product')}:{row.get('plant')}",
                    row.get("product"),
                    row.get("plant"),
                    row.get("availabilityCheckType"),
                    row.get("profitCenter"),
                    row.get("mrpType"),
                )
                for row in product_plant_rows
            ],
        )
        cur.executemany(
            "INSERT INTO product_storage_locations VALUES (?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('product')}:{row.get('plant')}:{row.get('storageLocation')}",
                    row.get("product"),
                    row.get("plant"),
                    row.get("storageLocation"),
                    row.get("physicalInventoryBlockInd"),
                    iso_to_date(row.get("dateOfLastPostedCntUnRstrcdStk")),
                )
                for row in product_storage_locations
            ],
        )

        item_rows: list[tuple[Any, ...]] = []
        order_plant_candidates: dict[str, list[str]] = defaultdict(list)
        for row in sales_items:
            item_id = f"{row['salesOrder']}-{row['salesOrderItem']}"
            item_rows.append(
                (
                    item_id,
                    row.get("salesOrder"),
                    row.get("salesOrderItem"),
                    row.get("material"),
                    to_float(row.get("requestedQuantity")),
                    row.get("requestedQuantityUnit"),
                    to_float(row.get("netAmount")),
                    row.get("materialGroup"),
                    row.get("productionPlant"),
                    row.get("storageLocation"),
                )
            )
            if row.get("productionPlant"):
                order_plant_candidates[row["salesOrder"]].append(row["productionPlant"])
        cur.executemany("INSERT INTO sales_order_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", item_rows)

        order_rows: list[tuple[Any, ...]] = []
        for row in sales_orders:
            plants_for_order = order_plant_candidates.get(row["salesOrder"], [])
            primary_plant = max(set(plants_for_order), key=plants_for_order.count) if plants_for_order else None
            order_rows.append(
                (
                    row.get("salesOrder"),
                    row.get("soldToParty"),
                    row.get("salesOrganization"),
                    row.get("distributionChannel"),
                    row.get("organizationDivision"),
                    iso_to_date(row.get("creationDate")),
                    iso_to_date(row.get("requestedDeliveryDate")),
                    to_float(row.get("totalNetAmount")),
                    row.get("transactionCurrency"),
                    row.get("overallDeliveryStatus"),
                    row.get("overallOrdReltdBillgStatus"),
                    row.get("incotermsClassification"),
                    row.get("incotermsLocation1"),
                    row.get("customerPaymentTerms"),
                    primary_plant,
                )
            )
        cur.executemany("INSERT INTO sales_orders VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", order_rows)
        cur.executemany(
            "INSERT INTO sales_order_schedule_lines VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('salesOrder')}:{row.get('salesOrderItem')}:{row.get('scheduleLine')}",
                    row.get("salesOrder"),
                    row.get("salesOrderItem"),
                    row.get("scheduleLine"),
                    iso_to_date(row.get("confirmedDeliveryDate")),
                    row.get("orderQuantityUnit"),
                    to_float(row.get("confdOrderQtyByMatlAvailCheck")),
                )
                for row in sales_schedule_lines
            ],
        )

        cur.executemany(
            "INSERT INTO deliveries VALUES (?, ?, ?, ?, ?)",
            [(row.get("deliveryDocument"), iso_to_date(row.get("creationDate")), row.get("shippingPoint"), row.get("overallGoodsMovementStatus"), row.get("overallPickingStatus")) for row in deliveries],
        )
        cur.executemany(
            "INSERT INTO delivery_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('deliveryDocument')}:{row.get('deliveryDocumentItem')}",
                    row.get("deliveryDocument"),
                    row.get("deliveryDocumentItem"),
                    row.get("referenceSdDocument"),
                    row.get("referenceSdDocumentItem"),
                    None,
                    row.get("plant"),
                    row.get("storageLocation"),
                    to_float(row.get("actualDeliveryQuantity")),
                    row.get("deliveryQuantityUnit"),
                )
                for row in delivery_items
            ],
        )
        cur.execute(
            """
            UPDATE delivery_items
            SET product_id = (
                SELECT soi.product_id
                FROM sales_order_items soi
                WHERE soi.sales_order_id = delivery_items.sales_order_id
                  AND soi.sales_order_item = ltrim(delivery_items.sales_order_item, '0')
                LIMIT 1
            )
            WHERE product_id IS NULL
            """
        )
        billing_map: dict[str, tuple[Any, ...]] = {}
        for row in billing_headers:
            billing_map[row.get("billingDocument")] = (
                row.get("billingDocument"),
                row.get("soldToParty"),
                row.get("companyCode"),
                row.get("fiscalYear"),
                row.get("accountingDocument"),
                iso_to_date(row.get("billingDocumentDate")),
                to_float(row.get("totalNetAmount")),
                row.get("transactionCurrency"),
                1 if row.get("billingDocumentIsCancelled") else 0,
            )
        for row in billings:
            billing_map[row.get("billingDocument")] = (
                row.get("billingDocument"),
                row.get("soldToParty"),
                row.get("companyCode"),
                row.get("fiscalYear"),
                row.get("accountingDocument"),
                iso_to_date(row.get("billingDocumentDate")),
                to_float(row.get("totalNetAmount")),
                row.get("transactionCurrency"),
                1 if row.get("billingDocumentIsCancelled") else 0,
            )
        cur.executemany("INSERT INTO billing_documents VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(billing_map.values()))
        cur.executemany(
            "INSERT INTO billing_document_items VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                (
                    f"{row.get('billingDocument')}:{row.get('billingDocumentItem')}",
                    row.get("billingDocument"),
                    row.get("billingDocumentItem"),
                    row.get("material"),
                    row.get("referenceSdDocument"),
                    row.get("referenceSdDocumentItem"),
                    to_float(row.get("billingQuantity")),
                    row.get("billingQuantityUnit"),
                    to_float(row.get("netAmount")),
                    row.get("transactionCurrency"),
                )
                for row in billing_items
            ],
        )
        journal_map: dict[str, tuple[Any, ...]] = {}
        for row in journal_entries:
            journal_map[row.get("accountingDocument")] = (
                row.get("accountingDocument"),
                row.get("customer"),
                row.get("companyCode"),
                row.get("fiscalYear"),
                row.get("referenceDocument"),
                row.get("glAccount"),
                row.get("accountingDocumentType"),
                iso_to_date(row.get("postingDate")),
                to_float(row.get("amountInTransactionCurrency")),
                row.get("clearingAccountingDocument"),
            )
        cur.executemany("INSERT INTO journal_entries VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", list(journal_map.values()))

        payment_map: dict[str, tuple[Any, ...]] = {}
        for row in payments:
            payment_id = row.get("clearingAccountingDocument") or f"{row.get('accountingDocument')}:{row.get('accountingDocumentItem')}"
            payment_map[payment_id] = (
                payment_id,
                row.get("accountingDocument"),
                row.get("customer"),
                row.get("companyCode"),
                row.get("fiscalYear"),
                row.get("clearingAccountingDocument"),
                iso_to_date(row.get("postingDate")),
                to_float(row.get("amountInTransactionCurrency")),
                row.get("transactionCurrency"),
            )
        cur.executemany("INSERT INTO payments VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", list(payment_map.values()))

        self._build_inferred_links(cur)
        self._build_graph(cur)
        conn.commit()
        conn.close()

    def _build_inferred_links(self, cur: sqlite3.Cursor) -> None:
        link_map: dict[tuple[str, str, str], tuple[str, str, str, float, str]] = {}

        def put_link(link_type: str, source_id: str | None, target_id: str | None, confidence: float, reason: str) -> None:
            if not source_id or not target_id:
                return
            key = (link_type, source_id, target_id)
            existing = link_map.get(key)
            if existing is None or confidence > existing[3]:
                link_map[key] = (link_type, source_id, target_id, confidence, reason)

        for row in cur.execute("SELECT DISTINCT sales_order_id, delivery_id FROM delivery_items WHERE sales_order_id IS NOT NULL AND delivery_id IS NOT NULL"):
            put_link("ORDER_TO_DELIVERY", row["sales_order_id"], row["delivery_id"], 1.0, "Direct link from outbound delivery items")

        for row in cur.execute("SELECT DISTINCT delivery_id, billing_document_id FROM billing_document_items WHERE delivery_id IS NOT NULL AND billing_document_id IS NOT NULL"):
            put_link("DELIVERY_TO_BILLING", row["delivery_id"], row["billing_document_id"], 1.0, "Direct link from billing document items")

        direct_order_billing_sql = """
            SELECT DISTINCT di.sales_order_id, bi.billing_document_id
            FROM delivery_items di
            JOIN billing_document_items bi
              ON bi.delivery_id = di.delivery_id
             AND (bi.delivery_item = di.delivery_item OR bi.delivery_item IS NULL OR di.delivery_item IS NULL)
            WHERE di.sales_order_id IS NOT NULL AND bi.billing_document_id IS NOT NULL
        """
        for row in cur.execute(direct_order_billing_sql):
            put_link("ORDER_TO_BILLING", row["sales_order_id"], row["billing_document_id"], 1.0, "Direct order to billing path via delivery and billing items")

        orders = [dict(row) for row in cur.execute("SELECT sales_order_id, customer_id, creation_date, total_net_amount, primary_plant FROM sales_orders ORDER BY customer_id, creation_date, total_net_amount, sales_order_id")]
        billings = [dict(row) for row in cur.execute("SELECT billing_document_id, customer_id, billing_date, total_net_amount FROM billing_documents ORDER BY customer_id, billing_date, total_net_amount, billing_document_id")]
        deliveries = [dict(row) for row in cur.execute("SELECT delivery_id, creation_date, shipping_point FROM deliveries ORDER BY creation_date, shipping_point, delivery_id")]

        unmatched_orders = [row for row in orders if not any(key[0] == "ORDER_TO_BILLING" and key[1] == row["sales_order_id"] for key in link_map)]
        unmatched_billings = [row for row in billings if not any(key[0] == "ORDER_TO_BILLING" and key[2] == row["billing_document_id"] for key in link_map)]

        order_groups: dict[tuple[str, str, float], list[dict[str, Any]]] = defaultdict(list)
        billing_groups: dict[tuple[str, str, float], list[dict[str, Any]]] = defaultdict(list)
        for row in unmatched_orders:
            if row["total_net_amount"] is not None:
                order_groups[(row["customer_id"], row["creation_date"], round(row["total_net_amount"], 2))].append(row)
        for row in unmatched_billings:
            if row["total_net_amount"] is not None:
                billing_groups[(row["customer_id"], row["billing_date"], round(row["total_net_amount"], 2))].append(row)
        for key, order_list in order_groups.items():
            billing_list = billing_groups.get(key, [])
            limit = min(len(order_list), len(billing_list))
            confidence = 0.95 if limit == len(order_list) == len(billing_list) else 0.74
            for index in range(limit):
                put_link("ORDER_TO_BILLING", order_list[index]["sales_order_id"], billing_list[index]["billing_document_id"], confidence, "Matched on customer, date, amount, and duplicate sequence")

        unmatched_delivery_orders = [row for row in orders if not any(key[0] == "ORDER_TO_DELIVERY" and key[1] == row["sales_order_id"] for key in link_map)]
        delivery_groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        order_groups_for_delivery: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in deliveries:
            if not any(key[0] == "ORDER_TO_DELIVERY" and key[2] == row["delivery_id"] for key in link_map):
                delivery_groups[(row["creation_date"], row["shipping_point"])].append(row)
        for row in unmatched_delivery_orders:
            if row["primary_plant"]:
                order_groups_for_delivery[(row["creation_date"], row["primary_plant"])].append(row)
        for key, order_list in order_groups_for_delivery.items():
            delivery_list = delivery_groups.get(key, [])
            for index in range(min(len(order_list), len(delivery_list))):
                put_link("ORDER_TO_DELIVERY", order_list[index]["sales_order_id"], delivery_list[index]["delivery_id"], 0.66, "Matched on date, inferred plant/shipping point, and sequence")

        for row in cur.execute("SELECT journal_entry_id, billing_document_id, clearing_accounting_document FROM journal_entries WHERE billing_document_id IS NOT NULL"):
            put_link("BILLING_TO_JOURNAL", row["billing_document_id"], row["journal_entry_id"], 1.0, "Direct billing reference on journal entry")
            if row["clearing_accounting_document"] and cur.execute("SELECT 1 FROM payments WHERE payment_id = ? LIMIT 1", (row["clearing_accounting_document"],)).fetchone():
                put_link("JOURNAL_TO_PAYMENT", row["journal_entry_id"], row["clearing_accounting_document"], 1.0, "Direct clearing accounting document match")

        cur.executemany("INSERT INTO inferred_flow_links VALUES (?, ?, ?, ?, ?)", list(link_map.values()))

    def _build_graph(self, cur: sqlite3.Cursor) -> None:
        configs = [
            ("customers", "customer_id", "Customer", lambda row: row["name"] or row["customer_id"], "customer"),
            ("customer_company_assignments", "assignment_id", "Customer Company Assignment", lambda row: f"{row['customer_id']} / {row['company_code']}", "customer_company_assignment"),
            ("customer_sales_area_assignments", "assignment_id", "Customer Sales Area", lambda row: f"{row['customer_id']} / {row['sales_organization']} / {row['distribution_channel']}", "customer_sales_area"),
            ("business_partner_addresses", "address_id", "Address", lambda row: f"{row['street_name']} {row['city_name']}".strip() or row["address_id"], "address"),
            ("sales_orders", "sales_order_id", "Sales Order", lambda row: row["sales_order_id"], "sales_order"),
            ("sales_order_items", "item_id", "Sales Order Item", lambda row: row["item_id"], "sales_order_item"),
            ("sales_order_schedule_lines", "schedule_line_id", "Schedule Line", lambda row: row["schedule_line_id"], "schedule_line"),
            ("products", "product_id", "Product", lambda row: row["description"] or row["product_id"], "product"),
            ("product_master", "product_id", "Product Master", lambda row: row["product_id"], "product_master"),
            ("product_plants", "product_plant_id", "Product Plant", lambda row: row["product_plant_id"], "product_plant"),
            ("product_storage_locations", "location_id", "Storage Location", lambda row: f"{row['plant_id']} / {row['storage_location']}", "storage_location"),
            ("plants", "plant_id", "Plant", lambda row: row["plant_name"] or row["plant_id"], "plant"),
            ("deliveries", "delivery_id", "Delivery", lambda row: row["delivery_id"], "delivery"),
            ("delivery_items", "delivery_item_id", "Delivery Item", lambda row: row["delivery_item_id"], "delivery_item"),
            ("billing_documents", "billing_document_id", "Billing Document", lambda row: row["billing_document_id"], "billing_document"),
            ("billing_document_items", "billing_item_id", "Billing Item", lambda row: row["billing_item_id"], "billing_item"),
            ("journal_entries", "journal_entry_id", "Journal Entry", lambda row: row["journal_entry_id"], "journal_entry"),
            ("payments", "payment_id", "Payment", lambda row: row["payment_id"], "payment"),
        ]
        for table, key, entity_type, label_fn, prefix in configs:
            rows = [dict(row) for row in cur.execute(f"SELECT * FROM {table}")]
            for row in rows:
                cur.execute("INSERT INTO graph_nodes VALUES (?, ?, ?, ?)", (f"{prefix}:{row[key]}", label_fn(row), entity_type, json.dumps(dict(row))))

        edge_rows = []
        edge_rows.extend([(f"customer-order:{r['customer_id']}:{r['sales_order_id']}", f"customer:{r['customer_id']}", f"sales_order:{r['sales_order_id']}", "PLACED_ORDER", 1.0, "{}") for r in cur.execute("SELECT customer_id, sales_order_id FROM sales_orders")])
        edge_rows.extend([(f"customer-company:{r['customer_id']}:{r['assignment_id']}", f"customer:{r['customer_id']}", f"customer_company_assignment:{r['assignment_id']}", "HAS_COMPANY_ASSIGNMENT", 1.0, "{}") for r in cur.execute("SELECT customer_id, assignment_id FROM customer_company_assignments")])
        edge_rows.extend([(f"customer-sales-area:{r['customer_id']}:{r['assignment_id']}", f"customer:{r['customer_id']}", f"customer_sales_area:{r['assignment_id']}", "HAS_SALES_AREA", 1.0, "{}") for r in cur.execute("SELECT customer_id, assignment_id FROM customer_sales_area_assignments")])
        edge_rows.extend([(f"customer-address:{r['customer_id']}:{r['address_id']}", f"customer:{r['customer_id']}", f"address:{r['address_id']}", "HAS_ADDRESS", 1.0, "{}") for r in cur.execute("SELECT customer_id, address_id FROM business_partner_addresses")])
        for row in cur.execute("SELECT sales_order_id, item_id, product_id, production_plant FROM sales_order_items"):
            edge_rows.append((f"order-item:{row['sales_order_id']}:{row['item_id']}", f"sales_order:{row['sales_order_id']}", f"sales_order_item:{row['item_id']}", "HAS_ITEM", 1.0, "{}"))
            if row["product_id"]:
                edge_rows.append((f"item-product:{row['item_id']}:{row['product_id']}", f"sales_order_item:{row['item_id']}", f"product:{row['product_id']}", "FOR_PRODUCT", 1.0, "{}"))
            if row["production_plant"]:
                edge_rows.append((f"item-plant:{row['item_id']}:{row['production_plant']}", f"sales_order_item:{row['item_id']}", f"plant:{row['production_plant']}", "FULFILLED_BY_PLANT", 1.0, "{}"))
        edge_rows.extend([(f"item-schedule:{r['item_id']}:{r['schedule_line_id']}", f"sales_order_item:{r['item_id']}", f"schedule_line:{r['schedule_line_id']}", "HAS_SCHEDULE_LINE", 1.0, "{}") for r in cur.execute("SELECT soi.item_id, sosl.schedule_line_id FROM sales_order_schedule_lines sosl JOIN sales_order_items soi ON soi.sales_order_id = sosl.sales_order_id AND soi.sales_order_item = sosl.sales_order_item")])
        edge_rows.extend([(f"product-master:{r['product_id']}", f"product:{r['product_id']}", f"product_master:{r['product_id']}", "HAS_PRODUCT_MASTER", 1.0, "{}") for r in cur.execute("SELECT product_id FROM product_master")])
        edge_rows.extend([(f"product-plant-node:{r['product_id']}:{r['product_plant_id']}", f"product:{r['product_id']}", f"product_plant:{r['product_plant_id']}", "PLANNED_IN_PLANT", 1.0, "{}") for r in cur.execute("SELECT product_id, product_plant_id FROM product_plants")])
        edge_rows.extend([(f"product-plant:{r['product_plant_id']}:{r['plant_id']}", f"product_plant:{r['product_plant_id']}", f"plant:{r['plant_id']}", "AT_PLANT", 1.0, "{}") for r in cur.execute("SELECT product_plant_id, plant_id FROM product_plants")])
        for row in cur.execute("SELECT location_id, product_id, plant_id FROM product_storage_locations"):
            edge_rows.append((f"product-storage:{row['product_id']}:{row['location_id']}", f"product:{row['product_id']}", f"storage_location:{row['location_id']}", "STORED_AT", 1.0, "{}"))
            if row["plant_id"]:
                edge_rows.append((f"storage-plant:{row['location_id']}:{row['plant_id']}", f"storage_location:{row['location_id']}", f"plant:{row['plant_id']}", "LOCATED_IN_PLANT", 1.0, "{}"))
        edge_rows.extend([(f"delivery-plant:{r['delivery_id']}:{r['shipping_point']}", f"delivery:{r['delivery_id']}", f"plant:{r['shipping_point']}", "SHIPS_FROM", 1.0, "{}") for r in cur.execute("SELECT delivery_id, shipping_point FROM deliveries WHERE shipping_point IS NOT NULL")])
        edge_rows.extend([(f"delivery-item:{r['delivery_id']}:{r['delivery_item_id']}", f"delivery:{r['delivery_id']}", f"delivery_item:{r['delivery_item_id']}", "HAS_DELIVERY_ITEM", 1.0, "{}") for r in cur.execute("SELECT delivery_id, delivery_item_id FROM delivery_items")])
        order_delivery_item_rows = cur.execute(
            """
            SELECT soi.item_id, di.delivery_item_id
            FROM delivery_items di
            JOIN sales_order_items soi
              ON soi.sales_order_id = di.sales_order_id
             AND soi.sales_order_item = ltrim(di.sales_order_item, '0')
            """
        )
        for row in order_delivery_item_rows:
            edge_rows.append((f"orderitem-deliveryitem:{row['item_id']}:{row['delivery_item_id']}", f"sales_order_item:{row['item_id']}", f"delivery_item:{row['delivery_item_id']}", "DELIVERED_AS", 1.0, "{}"))
        edge_rows.extend([(f"billing-customer:{r['billing_document_id']}:{r['customer_id']}", f"billing_document:{r['billing_document_id']}", f"customer:{r['customer_id']}", "BILLED_TO", 1.0, "{}") for r in cur.execute("SELECT billing_document_id, customer_id FROM billing_documents")])
        edge_rows.extend([(f"billing-item:{r['billing_document_id']}:{r['billing_item_id']}", f"billing_document:{r['billing_document_id']}", f"billing_item:{r['billing_item_id']}", "HAS_BILLING_ITEM", 1.0, "{}") for r in cur.execute("SELECT billing_document_id, billing_item_id FROM billing_document_items")])
        delivery_billing_item_rows = cur.execute(
            """
            SELECT di.delivery_item_id, bi.billing_item_id
            FROM billing_document_items bi
            JOIN delivery_items di
              ON di.delivery_id = bi.delivery_id
             AND di.delivery_item = bi.delivery_item
            """
        )
        for row in delivery_billing_item_rows:
            edge_rows.append((f"deliveryitem-billingitem:{row['delivery_item_id']}:{row['billing_item_id']}", f"delivery_item:{row['delivery_item_id']}", f"billing_item:{row['billing_item_id']}", "BILLED_AS", 1.0, "{}"))

        prefix_map = {"ORDER": "sales_order", "DELIVERY": "delivery", "BILLING": "billing_document", "JOURNAL": "journal_entry", "PAYMENT": "payment"}
        for row in cur.execute("SELECT * FROM inferred_flow_links WHERE target_id IS NOT NULL"):
            source_prefix = prefix_map[row["link_type"].split("_TO_")[0]]
            target_prefix = prefix_map[row["link_type"].split("_TO_")[1]]
            edge_rows.append((f"inferred:{row['link_type']}:{row['source_id']}:{row['target_id']}", f"{source_prefix}:{row['source_id']}", f"{target_prefix}:{row['target_id']}", row["link_type"], row["confidence"], json.dumps({"reason": row["reason"]})))
        cur.executemany("INSERT INTO graph_edges VALUES (?, ?, ?, ?, ?, ?)", edge_rows)

    def summary(self) -> dict[str, int]:
        conn = self.connect()
        cur = conn.cursor()
        data = {}
        for table in [
            "customers",
            "customer_company_assignments",
            "customer_sales_area_assignments",
            "business_partner_addresses",
            "sales_orders",
            "sales_order_items",
            "sales_order_schedule_lines",
            "products",
            "product_master",
            "product_plants",
            "product_storage_locations",
            "plants",
            "deliveries",
            "delivery_items",
            "billing_documents",
            "billing_document_items",
            "journal_entries",
            "payments",
        ]:
            data[table] = cur.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        conn.close()
        return data

    def search_nodes(self, term: str) -> list[dict[str, Any]]:
        conn = self.connect()
        cur = conn.cursor()
        like = f"%{term.lower()}%"
        rows = [dict(row) for row in cur.execute("SELECT node_id, label, entity_type FROM graph_nodes WHERE lower(node_id) LIKE ? OR lower(label) LIKE ? OR lower(metadata_json) LIKE ? LIMIT 12", (like, like, like))]
        conn.close()
        return rows

    def node(self, node_id: str) -> dict[str, Any]:
        conn = self.connect()
        cur = conn.cursor()
        row = cur.execute("SELECT * FROM graph_nodes WHERE node_id = ?", (node_id,)).fetchone()
        conn.close()
        if not row:
            raise HTTPException(status_code=404, detail="Node not found")
        record = dict(row)
        record["metadata"] = json.loads(record.pop("metadata_json"))
        return record

    def entity_type_for_node(self, node_id: str) -> str | None:
        conn = self.connect()
        cur = conn.cursor()
        row = cur.execute("SELECT entity_type FROM graph_nodes WHERE node_id = ?", (node_id,)).fetchone()
        conn.close()
        return row["entity_type"] if row else None

    def neighborhood(self, node_id: str, include_details: bool = False) -> dict[str, Any]:
        conn = self.connect()
        cur = conn.cursor()
        center_row = cur.execute("SELECT entity_type FROM graph_nodes WHERE node_id = ?", (node_id,)).fetchone()
        if not center_row:
            conn.close()
            raise HTTPException(status_code=404, detail="Node not found")
        center_type = center_row["entity_type"]
        edges = []
        for row in cur.execute("SELECT * FROM graph_edges WHERE source_id = ? OR target_id = ?", (node_id, node_id)):
            edge = dict(row)
            edge["metadata"] = json.loads(edge.pop("metadata_json"))
            edges.append(edge)
        ids = {node_id}
        for edge in edges:
            ids.add(edge["source_id"])
            ids.add(edge["target_id"])
        placeholders = ",".join("?" for _ in ids)
        nodes = []
        for row in cur.execute(f"SELECT * FROM graph_nodes WHERE node_id IN ({placeholders})", tuple(ids)):
            record = dict(row)
            record["metadata"] = json.loads(record.pop("metadata_json"))
            nodes.append(record)
        if not include_details and center_type in BUSINESS_ENTITY_TYPES:
            allowed_ids = {node_id}
            allowed_nodes = []
            hidden_count = 0
            granular_ids = {node["node_id"] for node in nodes if node["entity_type"] in GRANULAR_ENTITY_TYPES}
            for node in nodes:
                if node["node_id"] == node_id or node["entity_type"] in BUSINESS_ENTITY_TYPES:
                    allowed_nodes.append(node)
                    allowed_ids.add(node["node_id"])
                else:
                    hidden_count += 1
            filtered_edges = []
            for edge in edges:
                if edge["source_id"] in allowed_ids and edge["target_id"] in allowed_ids:
                    filtered_edges.append(edge)
                elif edge["source_id"] == node_id and edge["target_id"] in granular_ids:
                    hidden_count += 0
                elif edge["target_id"] == node_id and edge["source_id"] in granular_ids:
                    hidden_count += 0
            nodes = allowed_nodes
            edges = filtered_edges
        else:
            hidden_count = 0
        conn.close()
        return {"nodes": nodes, "edges": edges, "center": node_id, "include_details": include_details, "hidden_count": hidden_count}

    def execute_sql(self, sql: str, params: tuple[Any, ...] = ()) -> list[dict[str, Any]]:
        conn = self.connect()
        cur = conn.cursor()
        rows = [dict(row) for row in cur.execute(sql, params)]
        conn.close()
        return rows

    def detect_entity_type(self, entity_id: str) -> str | None:
        conn = self.connect()
        cur = conn.cursor()
        checks = [
            ("sales_orders", "sales_order_id", "sales_order"),
            ("deliveries", "delivery_id", "delivery"),
            ("billing_documents", "billing_document_id", "billing_document"),
            ("journal_entries", "journal_entry_id", "journal_entry"),
            ("payments", "payment_id", "payment"),
            ("customers", "customer_id", "customer"),
            ("plants", "plant_id", "plant"),
            ("products", "product_id", "product"),
            ("business_partner_addresses", "address_id", "address"),
            ("product_master", "product_id", "product_master"),
            ("product_plants", "product_plant_id", "product_plant"),
            ("customer_company_assignments", "assignment_id", "customer_company_assignment"),
            ("customer_sales_area_assignments", "assignment_id", "customer_sales_area"),
            ("product_storage_locations", "location_id", "storage_location"),
            ("sales_order_schedule_lines", "schedule_line_id", "schedule_line"),
            ("delivery_items", "delivery_item_id", "delivery_item"),
            ("billing_document_items", "billing_item_id", "billing_item"),
        ]
        entity_type = None
        for table, column, resolved in checks:
            if cur.execute(f"SELECT 1 FROM {table} WHERE {column} = ? LIMIT 1", (entity_id,)).fetchone():
                entity_type = resolved
                break
        conn.close()
        return entity_type


class LLMClient:
    def __init__(self) -> None:
        self.provider = os.getenv("LLM_PROVIDER", "").lower().strip()

    def available(self) -> bool:
        return bool(self.provider and self._api_key())

    def _api_key(self) -> str | None:
        return {"groq": os.getenv("GROQ_API_KEY"), "openrouter": os.getenv("OPENROUTER_API_KEY"), "gemini": os.getenv("GEMINI_API_KEY")}.get(self.provider)

    def generate_sql(self, prompt: str) -> str | None:
        if not self.available():
            return None
        if self.provider == "groq":
            payload = {"model": os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile").strip(), "messages": [{"role": "system", "content": "Return only a single SQL SELECT statement."}, {"role": "user", "content": prompt}], "temperature": 0.1}
            req = request.Request("https://api.groq.com/openai/v1/chat/completions", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json", "Authorization": f"Bearer {self._api_key()}"})
        elif self.provider == "openrouter":
            payload = {"model": os.getenv("OPENROUTER_MODEL", "meta-llama/llama-3.3-70b-instruct:free").strip(), "messages": [{"role": "system", "content": "Return only a single SQL SELECT statement."}, {"role": "user", "content": prompt}], "temperature": 0.1}
            req = request.Request("https://openrouter.ai/api/v1/chat/completions", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json", "Authorization": f"Bearer {self._api_key()}"})
        else:
            model = os.getenv("GEMINI_MODEL", "gemini-2.0-flash").strip()
            payload = {"contents": [{"parts": [{"text": prompt}]}], "generationConfig": {"temperature": 0.1}}
            req = request.Request(f"https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={self._api_key()}", data=json.dumps(payload).encode("utf-8"), headers={"Content-Type": "application/json"})
        try:
            with request.urlopen(req, timeout=30) as response:
                body = json.loads(response.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError, json.JSONDecodeError, KeyError, IndexError, ValueError):
            return None
        if self.provider == "gemini":
            return body.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text")
        return body.get("choices", [{}])[0].get("message", {}).get("content")


class QueryEngine:
    def __init__(self, repo: O2CRepository, llm: LLMClient) -> None:
        self.repo = repo
        self.llm = llm

    def run(self, message: str) -> dict[str, Any]:
        if not self._is_domain_question(message):
            return self._reject()
        deterministic = self._run_deterministic(message)
        if deterministic:
            return deterministic
        llm_sql = self._run_llm(message)
        if llm_sql:
            return llm_sql
        if self.llm.available():
            return {"answer": "I could not map that request to a safe data query yet. Try asking about sales orders, deliveries, billing documents, journal entries, payments, products, plants, customers, or addresses.", "sql": "-- no safe query generated", "rows": [], "highlights": [], "mode": "fallback"}
        return {"answer": "I could not map that request to a safe data query yet, and no LLM provider is configured. Try one of the built-in business questions or add a free-tier API key.", "sql": "-- no safe query generated", "rows": [], "highlights": [], "mode": "fallback"}

    def _is_domain_question(self, message: str) -> bool:
        lowered = message.lower()
        if any(term in lowered for term in ["poem", "joke", "weather", "capital of", "recipe", "movie", "story", "translate", "essay", "fiction"]):
            return False
        return any(term in lowered for term in ["sales order", "delivery", "billing", "invoice", "payment", "journal", "customer", "product", "plant", "order to cash", "flow", "document"]) or bool(re.search(r"\b\d{6,10}\b", message))

    @staticmethod
    def _summarize_rows(rows: list[dict[str, Any]]) -> str:
        if not rows:
            return "No rows matched the query."
        first = rows[0]
        if len(first) == 1:
            key = next(iter(first))
            sample = ", ".join(str(row[key]) for row in rows[:5])
            return f"I found {len(rows)} matching rows. Sample values: {sample}."
        preview_parts = []
        for key, value in list(first.items())[:3]:
            preview_parts.append(f"{key}={value}")
        return f"I found {len(rows)} matching rows. First row: {', '.join(preview_parts)}."

    def _reject(self, reason: str = "This system is designed to answer questions related to the provided SAP order-to-cash dataset only.") -> dict[str, Any]:
        return {"answer": reason, "sql": "-- rejected as out of domain", "rows": [], "highlights": [], "mode": "guardrail"}

    def _sanitize_sql(self, sql: str) -> str | None:
        candidate = sql.strip().strip("`")
        if candidate.lower().startswith("sql"):
            candidate = candidate[3:].strip()
        candidate = re.sub(r"--.*?$", "", candidate, flags=re.MULTILINE).strip()
        if candidate.endswith(";"):
            candidate = candidate[:-1].strip()
        if ";" in candidate:
            return None
        lowered = candidate.lower()
        if lowered.startswith("with "):
            return None
        if not lowered.startswith("select"):
            return None
        forbidden = ["insert ", "update ", "delete ", "drop ", "alter ", "attach ", "pragma ", "create ", "replace ", "truncate ", "vacuum ", "sqlite_master", "union "]
        if any(token in lowered for token in forbidden):
            return None
        referenced = re.findall(r"\b(?:from|join)\s+([a-zA-Z_][a-zA-Z0-9_]*)", lowered)
        if not referenced:
            return None
        if any(name not in ALLOWED_SQL_TABLES for name in referenced):
            return None
        if " limit " not in lowered:
            candidate = f"{candidate} LIMIT 200"
        return candidate

    def _run_deterministic(self, message: str) -> dict[str, Any] | None:
        lowered = message.lower()
        if "highest number of billing" in lowered and "product" in lowered:
            sql = """
                SELECT p.product_id, COALESCE(p.description, p.product_id) AS product_name, COUNT(DISTINCT bi.billing_document_id) AS billing_document_count
                FROM billing_document_items bi
                LEFT JOIN products p ON p.product_id = bi.product_id
                GROUP BY p.product_id, product_name
                ORDER BY billing_document_count DESC, product_name
                LIMIT 10
            """
            rows = self.repo.execute_sql(sql)
            answer = "Top products by linked billing documents:\n" + "\n".join([f"{index + 1}. {row['product_name']} ({row['product_id']}): {row['billing_document_count']} billing documents" for index, row in enumerate(rows[:5])])
            return {"answer": answer, "sql": sql.strip(), "rows": rows, "highlights": [f"product:{row['product_id']}" for row in rows[:5] if row["product_id"]], "mode": "deterministic"}

        candidate_ids = re.findall(r"\b\d{6,10}\b", message)
        billing_id = next((candidate for candidate in candidate_ids if self.repo.detect_entity_type(candidate) == "billing_document"), None)
        if billing_id and any(term in lowered for term in ["trace", "flow", "journal", "linked"]):
            sql = """
                SELECT
                    b.billing_document_id,
                    b.customer_id,
                    COALESCE(so.sales_order_id, o.sales_order_id) AS sales_order_id,
                    COALESCE(di.delivery_id, d.delivery_id) AS delivery_id,
                    j.journal_entry_id,
                    p.payment_id,
                    b.total_net_amount,
                    b.billing_date,
                    CASE
                        WHEN so.sales_order_id IS NOT NULL THEN 1.0
                        ELSE ob.confidence
                    END AS order_billing_confidence,
                    CASE
                        WHEN di.delivery_id IS NOT NULL THEN 1.0
                        ELSE od.confidence
                    END AS order_delivery_confidence,
                    di.delivery_id AS direct_delivery_id,
                    CASE
                        WHEN so.sales_order_id IS NOT NULL AND di.delivery_id IS NOT NULL THEN 'direct'
                        WHEN di.delivery_id IS NOT NULL THEN 'mixed'
                        ELSE 'inferred'
                    END AS flow_source
                FROM billing_documents b
                LEFT JOIN billing_document_items bi ON bi.billing_document_id = b.billing_document_id
                LEFT JOIN delivery_items di
                  ON di.delivery_id = bi.delivery_id
                 AND ltrim(di.delivery_item, '0') = ltrim(bi.delivery_item, '0')
                LEFT JOIN sales_orders so ON so.sales_order_id = di.sales_order_id
                LEFT JOIN inferred_flow_links ob ON ob.link_type = 'ORDER_TO_BILLING' AND ob.target_id = b.billing_document_id
                LEFT JOIN sales_orders o ON o.sales_order_id = ob.source_id
                LEFT JOIN inferred_flow_links od ON od.link_type = 'ORDER_TO_DELIVERY' AND od.source_id = o.sales_order_id
                LEFT JOIN deliveries d ON d.delivery_id = od.target_id
                LEFT JOIN journal_entries j ON j.billing_document_id = b.billing_document_id
                LEFT JOIN payments p ON p.payment_id = j.clearing_accounting_document
                WHERE b.billing_document_id = ?
                LIMIT 1
            """
            rows = self.repo.execute_sql(sql, (billing_id,))
            if not rows:
                return {"answer": f"I could not find billing document {billing_id} in the dataset.", "sql": sql.strip(), "rows": [], "highlights": [], "mode": "deterministic"}
            row = rows[0]
            delivery_id = row["delivery_id"]
            source_note = {
                "direct": "using direct item-level links end-to-end",
                "mixed": "using direct billing-to-delivery links plus inferred upstream order mapping",
                "inferred": "using inferred upstream links where direct references were unavailable",
            }[row["flow_source"]]
            answer = f"Billing document {billing_id} belongs to customer {row['customer_id']} and amount {row['total_net_amount']} on {row['billing_date']}. Flow: sales order {row['sales_order_id'] or 'not linked'} -> delivery {delivery_id or 'not linked'} -> billing {billing_id} -> journal entry {row['journal_entry_id'] or 'not found'} -> payment {row['payment_id'] or 'not found'}, {source_note}."
            highlights = [f"billing_document:{billing_id}"] + ([f"sales_order:{row['sales_order_id']}"] if row["sales_order_id"] else []) + ([f"delivery:{delivery_id}"] if delivery_id else []) + ([f"journal_entry:{row['journal_entry_id']}"] if row["journal_entry_id"] else []) + ([f"payment:{row['payment_id']}"] if row["payment_id"] else [])
            return {"answer": answer, "sql": sql.strip(), "rows": rows, "highlights": highlights, "mode": "deterministic"}

        if "broken" in lowered or "incomplete flow" in lowered or "delivered but not billed" in lowered or "billed without delivery" in lowered:
            sql = """
                WITH order_flow AS (
                    SELECT o.sales_order_id, o.customer_id, o.creation_date, o.total_net_amount, od.target_id AS delivery_id, ob.target_id AS billing_document_id
                    FROM sales_orders o
                    LEFT JOIN inferred_flow_links od ON od.link_type = 'ORDER_TO_DELIVERY' AND od.source_id = o.sales_order_id
                    LEFT JOIN inferred_flow_links ob ON ob.link_type = 'ORDER_TO_BILLING' AND ob.source_id = o.sales_order_id
                )
                SELECT sales_order_id, customer_id, creation_date, total_net_amount, delivery_id, billing_document_id,
                       CASE
                           WHEN delivery_id IS NOT NULL AND billing_document_id IS NULL THEN 'Delivered but not billed'
                           WHEN delivery_id IS NULL AND billing_document_id IS NOT NULL THEN 'Billed without inferred delivery'
                           WHEN delivery_id IS NULL AND billing_document_id IS NULL THEN 'Order without delivery or billing'
                       END AS issue
                FROM order_flow
                WHERE delivery_id IS NULL OR billing_document_id IS NULL
                ORDER BY creation_date, sales_order_id
                LIMIT 25
            """
            rows = self.repo.execute_sql(sql)
            issue_counts: dict[str, int] = defaultdict(int)
            for row in rows:
                issue_counts[row["issue"]] += 1
            summary = ", ".join(f"{issue}: {count}" for issue, count in issue_counts.items())
            return {"answer": f"I found {len(rows)} incomplete flow examples in the first result page. Breakdown: {summary}.", "sql": sql.strip(), "rows": rows, "highlights": [f"sales_order:{row['sales_order_id']}" for row in rows[:10]], "mode": "deterministic"}

        customer_match = re.search(r"\b(3\d{8})\b", message)
        if customer_match and "address" in lowered:
            customer_id = customer_match.group(1)
            sql = """
                SELECT c.customer_id, c.name, a.address_id, a.street_name, a.city_name, a.region, a.postal_code, a.country
                FROM customers c
                LEFT JOIN business_partner_addresses a
                  ON a.customer_id = c.customer_id
                WHERE c.customer_id = ?
            """
            rows = self.repo.execute_sql(sql, (customer_id,))
            if rows:
                row = rows[0]
                answer = f"Customer {customer_id} is linked to address {row['address_id']}: {row['street_name']}, {row['city_name']}, {row['region']} {row['postal_code']}, {row['country']}."
                highlights = [f"customer:{customer_id}"] + ([f"address:{row['address_id']}"] if row["address_id"] else [])
                return {"answer": answer, "sql": sql.strip(), "rows": rows, "highlights": highlights, "mode": "deterministic"}

        entity_id = next(iter(candidate_ids), None)
        if entity_id:
            entity_type = self.repo.detect_entity_type(entity_id)
            mapping = {
                "sales_order": ("SELECT * FROM sales_orders WHERE sales_order_id = ?", "sales order"),
                "delivery": ("SELECT * FROM deliveries WHERE delivery_id = ?", "delivery"),
                "billing_document": ("SELECT * FROM billing_documents WHERE billing_document_id = ?", "billing document"),
                "journal_entry": ("SELECT * FROM journal_entries WHERE journal_entry_id = ?", "journal entry"),
                "payment": ("SELECT * FROM payments WHERE payment_id = ?", "payment"),
                "customer": ("SELECT * FROM customers WHERE customer_id = ?", "customer"),
                "plant": ("SELECT * FROM plants WHERE plant_id = ?", "plant"),
                "product": ("SELECT * FROM products WHERE product_id = ?", "product"),
                "address": ("SELECT * FROM business_partner_addresses WHERE address_id = ?", "address"),
                "product_master": ("SELECT * FROM product_master WHERE product_id = ?", "product master"),
                "product_plant": ("SELECT * FROM product_plants WHERE product_plant_id = ?", "product plant"),
                "customer_company_assignment": ("SELECT * FROM customer_company_assignments WHERE assignment_id = ?", "customer company assignment"),
                "customer_sales_area": ("SELECT * FROM customer_sales_area_assignments WHERE assignment_id = ?", "customer sales area assignment"),
                "storage_location": ("SELECT * FROM product_storage_locations WHERE location_id = ?", "storage location"),
                "schedule_line": ("SELECT * FROM sales_order_schedule_lines WHERE schedule_line_id = ?", "schedule line"),
                "delivery_item": ("SELECT * FROM delivery_items WHERE delivery_item_id = ?", "delivery item"),
                "billing_item": ("SELECT * FROM billing_document_items WHERE billing_item_id = ?", "billing item"),
            }
            if not entity_type:
                return {"answer": f"I could not find document {entity_id} in the dataset.", "sql": "-- no matching entity found", "rows": [], "highlights": [], "mode": "deterministic"}
            sql, label = mapping[entity_type]
            highlight = [f"{entity_type}:{entity_id}"]
            rows = self.repo.execute_sql(sql, (entity_id,))
            if rows:
                return {"answer": f"I found {label} {entity_id}. The record details are in the result table and the node has been highlighted.", "sql": sql, "rows": rows, "highlights": highlight, "mode": "deterministic"}
            return {"answer": f"I could not find document {entity_id} in the dataset.", "sql": sql, "rows": [], "highlights": [], "mode": "deterministic"}
        return None

    def _run_llm(self, message: str) -> dict[str, Any] | None:
        if not self.llm.available():
            return None
        prompt = f"""
You are querying a SQLite database for an SAP order-to-cash graph app.
Only generate a single SQLite SELECT statement.
Rules:
- Use only the listed tables.
- Never use WITH, UNION, PRAGMA, ATTACH, INSERT, UPDATE, DELETE, DROP, ALTER, or sqlite_master.
- Prefer direct document joins through delivery_items and billing_document_items when tracing business flows.
- Always include a LIMIT unless the user explicitly asks for an aggregate.
Tables:
- customers(customer_id, name, category, grouping_code, created_on, blocked)
- customer_company_assignments(assignment_id, customer_id, company_code, payment_terms, reconciliation_account, deletion_indicator, customer_account_group)
- customer_sales_area_assignments(assignment_id, customer_id, sales_organization, distribution_channel, division, currency, customer_payment_terms, incoterms, incoterms_location, shipping_condition, supplying_plant)
- business_partner_addresses(address_id, customer_id, city_name, country, postal_code, region, street_name)
- plants(plant_id, plant_name, address_id, distribution_channel, division)
- products(product_id, description)
- product_master(product_id, product_type, product_group, base_unit, gross_weight, net_weight, weight_unit, division)
- product_plants(product_plant_id, product_id, plant_id, availability_check_type, profit_center, mrp_type)
- sales_orders(sales_order_id, customer_id, sales_organization, distribution_channel, division, creation_date, requested_delivery_date, total_net_amount, currency, overall_delivery_status, billing_status, incoterms, incoterms_location, payment_terms, primary_plant)
- sales_order_items(item_id, sales_order_id, sales_order_item, product_id, requested_quantity, quantity_unit, net_amount, material_group, production_plant, storage_location)
- sales_order_schedule_lines(schedule_line_id, sales_order_id, sales_order_item, schedule_line, confirmed_delivery_date, order_quantity_unit, confirmed_quantity)
- deliveries(delivery_id, creation_date, shipping_point, goods_movement_status, picking_status)
- delivery_items(delivery_item_id, delivery_id, delivery_item, sales_order_id, sales_order_item, product_id, plant_id, storage_location, delivery_quantity, delivery_quantity_unit)
- billing_documents(billing_document_id, customer_id, company_code, fiscal_year, accounting_document_id, billing_date, total_net_amount, currency, cancelled)
- billing_document_items(billing_item_id, billing_document_id, billing_document_item, product_id, delivery_id, delivery_item, billing_quantity, billing_quantity_unit, net_amount, currency)
- journal_entries(journal_entry_id, customer_id, company_code, fiscal_year, billing_document_id, gl_account, accounting_document_type, posting_date, amount, clearing_accounting_document)
- payments(payment_id, accounting_document_id, customer_id, company_code, fiscal_year, clearing_accounting_document, posting_date, amount, currency)
- inferred_flow_links(link_type, source_id, target_id, confidence, reason)
Request: {message}
"""
        sql = self.llm.generate_sql(prompt)
        if not sql:
            return {"answer": "The configured LLM provider did not return a usable SQL query. Try rephrasing the request or use one of the built-in business questions.", "sql": "-- llm provider unavailable or returned no safe SQL", "rows": [], "highlights": [], "mode": "llm_fallback"}
        sql = self._sanitize_sql(sql)
        if not sql:
            return {"answer": "The LLM response was rejected by SQL safety checks. Try a narrower question about the dataset entities and flows.", "sql": "-- llm sql rejected by safety checks", "rows": [], "highlights": [], "mode": "llm_fallback"}
        try:
            rows = self.repo.execute_sql(sql)
        except sqlite3.Error:
            return {"answer": "The LLM generated a query that SQLite could not execute for this dataset. Try rephrasing the request.", "sql": sql, "rows": [], "highlights": [], "mode": "llm_fallback"}
        return {"answer": self._summarize_rows(rows), "sql": sql, "rows": rows, "highlights": [], "mode": "llm"}


repo = O2CRepository(DB_PATH)
query_engine = QueryEngine(repo, LLMClient())
app = FastAPI(title="Order-to-Cash Context Graph")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
def root() -> FileResponse:
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/summary")
def api_summary() -> dict[str, int]:
    return repo.summary()


@app.get("/api/search")
def api_search(q: str) -> list[dict[str, Any]]:
    return repo.search_nodes(q)


@app.get("/api/node/{node_id:path}")
def api_node(node_id: str) -> dict[str, Any]:
    return repo.node(node_id)


@app.get("/api/neighborhood/{node_id:path}")
def api_neighborhood(node_id: str, include_details: bool = False) -> dict[str, Any]:
    return repo.neighborhood(node_id, include_details=include_details)


@app.post("/api/chat")
def api_chat(payload: ChatRequest) -> dict[str, Any]:
    result = query_engine.run(payload.message)
    result["timestamp"] = datetime.utcnow().isoformat() + "Z"
    return result
