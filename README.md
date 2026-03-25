---
title: O2C Context Graph
emoji: 📊
colorFrom: blue
colorTo: green
sdk: docker
app_port: 8000
---

# Order-to-Cash Context Graph

A FastAPI-based context graph system for the SAP order-to-cash dataset. It ingests the full dataset into SQLite, builds graph nodes and relationships across business documents and master data, renders an exploratory graph UI, and exposes a guarded conversational interface for data-backed answers.

## Architecture

- Backend: FastAPI in [app.py](app.py)
- Storage: SQLite at `data/o2c_graph.db`
- Frontend: plain HTML, CSS, and JavaScript in [static/index.html](static/index.html)
- Query layer:
  - deterministic handlers for the core assignment questions
  - optional LLM-to-SQL path using free-tier Groq, OpenRouter, or Gemini keys

## Why SQLite

SQLite keeps the submission simple, portable, and easy to deploy. It is fast enough for this dataset size, needs no external database service, supports relational joins for document-flow queries, and lets the graph layer and chat layer share one grounded source of truth.

## Dataset Coverage

Integrated folders:

- `billing_document_cancellations`
- `billing_document_headers`
- `billing_document_items`
- `business_partners`
- `business_partner_addresses`
- `customer_company_assignments`
- `customer_sales_area_assignments`
- `journal_entry_items_accounts_receivable`
- `outbound_delivery_headers`
- `outbound_delivery_items`
- `payments_accounts_receivable`
- `plants`
- `products`
- `product_descriptions`
- `product_plants`
- `product_storage_locations`
- `sales_order_headers`
- `sales_order_items`
- `sales_order_schedule_lines`

## Graph Model

Primary business nodes:

- Customer
- Address
- Sales Order
- Sales Order Item
- Schedule Line
- Delivery
- Delivery Item
- Billing Document
- Billing Item
- Journal Entry
- Payment
- Product
- Product Master
- Plant
- Product Plant
- Storage Location

Direct relationships from source data include:

- `Customer -> Sales Order`
- `Customer -> Address`
- `Sales Order -> Sales Order Item`
- `Sales Order Item -> Schedule Line`
- `Sales Order Item -> Product`
- `Sales Order Item -> Plant`
- `Delivery -> Delivery Item`
- `Delivery Item -> Sales Order Item`
- `Billing Document -> Billing Item`
- `Billing Item -> Delivery Item`
- `Billing Document -> Journal Entry`
- `Journal Entry -> Payment`
- `Product -> Product Plant`
- `Product -> Storage Location`
- `Storage Location -> Plant`

Derived flow relationships are also materialized in `inferred_flow_links`:

- `ORDER_TO_DELIVERY`
- `DELIVERY_TO_BILLING`
- `ORDER_TO_BILLING`
- `BILLING_TO_JOURNAL`
- `JOURNAL_TO_PAYMENT`

Where direct item-level references exist, those links are built with confidence `1.0`. Fallback heuristic links are only used where the source dataset does not expose a clean key.

## LLM Prompting Strategy

The app uses a two-layer query approach:

1. Deterministic query handlers for high-value assignment questions.
2. Optional LLM-to-SQL generation for broader natural language questions.

The LLM prompt includes:

- the allowed SQLite tables
- the allowed columns
- an instruction to return only one `SELECT` statement
- a ban on mutating SQL

This keeps the generated query grounded in the local dataset.

## Guardrails

The chat layer rejects unrelated prompts such as general knowledge, jokes, poems, recipes, and other off-domain requests.

Example rejection:

`This system is designed to answer questions related to the provided SAP order-to-cash dataset only.`

SQL safety checks also reject statements that are not `SELECT` queries or that include mutation keywords like `INSERT`, `UPDATE`, `DELETE`, `DROP`, or `ALTER`.

## Example Questions

- `Which products are associated with the highest number of billing documents?`
- `Trace the full flow of billing document 90504298`
- `Identify sales orders that have broken or incomplete flows`
- `Find the address for customer 310000108`

## Run Locally

```bash
python -m uvicorn app:app --reload
```

Then open `http://127.0.0.1:8000`.

## Run Tests

```bash
python -m unittest discover -s tests -v
```

This validates:

- dataset table coverage
- direct billing-flow tracing
- guardrail rejection
- address lookup
- graph decluttering behavior
- SQL sanitization

## Environment Variables

See [.env.example](.env.example) for supported free-tier providers:

- Groq
- OpenRouter
- Gemini

If no API key is present, the app still works through deterministic grounded queries.

To validate the live LLM path after setting a key:

```bash
python validate_llm.py
```

## Deployment

The app is prepared for deployment as a Hugging Face Docker Space.

- [Dockerfile](Dockerfile)

### Hugging Face Spaces

This project can be deployed as a Docker Space for a no-card public demo path.

Suggested Hugging Face flow:

1. Create a new Space on Hugging Face.
2. Choose `Docker` as the SDK.
3. Push this repository to the Space.
4. In the Space settings, add secrets for:
   - `LLM_PROVIDER`
   - `GROQ_API_KEY`
   - `GROQ_MODEL`
5. Wait for the image build and startup to complete.
6. Verify the public Space URL, `/api/summary`, and the chat UI.

Notes:

- The included `Dockerfile` already serves FastAPI on port `8000`.
- The README metadata block at the top sets `sdk: docker` and `app_port: 8000`, which Hugging Face uses for the Space configuration.
- The app rebuilds `data/o2c_graph.db` from the bundled dataset when needed, so first startup can take a little longer.
- Real API keys should stay in Space secrets or local `.env`, never in `.env.example`.

## Submission Checklist

- Deploy the app and capture a public demo link
- Push the code to a public GitHub repository
- Include AI session logs or transcripts
- Include this README with architecture, database, prompting, and guardrails documented
- Include [AI_SESSION_LOG.md](AI_SESSION_LOG.md) in the submission bundle
