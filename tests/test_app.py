import unittest
from fastapi.testclient import TestClient

import app


class O2CAppTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.client = TestClient(app.app)

    def test_summary_includes_full_dataset_tables(self) -> None:
        data = self.client.get("/api/summary").json()
        self.assertIn("billing_document_items", data)
        self.assertIn("delivery_items", data)
        self.assertGreater(data["billing_document_items"], 0)
        self.assertGreater(data["delivery_items"], 0)

    def test_trace_billing_prefers_direct_flow(self) -> None:
        payload = self.client.post("/api/chat", json={"message": "Trace the full flow of billing document 90504219"}).json()
        row = payload["rows"][0]
        self.assertEqual(payload["mode"], "deterministic")
        self.assertEqual(row["sales_order_id"], "740520")
        self.assertEqual(row["delivery_id"], "80738051")
        self.assertEqual(row["flow_source"], "direct")
        self.assertEqual(row["order_billing_confidence"], 1.0)
        self.assertEqual(row["order_delivery_confidence"], 1.0)

    def test_guardrail_rejects_off_topic_prompt(self) -> None:
        payload = self.client.post("/api/chat", json={"message": "write me a story about dragons"}).json()
        self.assertEqual(payload["mode"], "guardrail")
        self.assertIn("provided SAP order-to-cash dataset only", payload["answer"])

    def test_address_lookup_returns_address_node(self) -> None:
        payload = self.client.post("/api/chat", json={"message": "Find the address for customer 310000108"}).json()
        self.assertIn("0171 Rebecca Glen", payload["answer"])
        self.assertIn("address:4605", payload["highlights"])

    def test_default_neighborhood_hides_granular_nodes(self) -> None:
        payload = self.client.get("/api/neighborhood/billing_document:90504298").json()
        detailed = self.client.get("/api/neighborhood/billing_document:90504298?include_details=true").json()
        self.assertGreater(payload["hidden_count"], 0)
        self.assertGreater(len(detailed["nodes"]), len(payload["nodes"]))

    def test_sql_sanitizer_rejects_unsafe_queries(self) -> None:
        self.assertIsNone(app.query_engine._sanitize_sql("SELECT * FROM sales_orders; DROP TABLE sales_orders;"))
        self.assertIsNone(app.query_engine._sanitize_sql("WITH x AS (SELECT 1) SELECT * FROM sales_orders"))
        self.assertEqual(
            app.query_engine._sanitize_sql("SELECT sales_order_id FROM sales_orders"),
            "SELECT sales_order_id FROM sales_orders LIMIT 200",
        )


if __name__ == "__main__":
    unittest.main()
