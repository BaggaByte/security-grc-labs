# ingest_risk.py
from elasticsearch import Elasticsearch, ApiError
from datetime import datetime, timezone

# -----------------------------
# 1️⃣ Connect to Elasticsearch with API key
# -----------------------------
es = Elasticsearch(
    "https://localhost:9200",
    api_key="TWVVczBwc0JzRUxWNF9FZW5DRDg6V1V5U3Y3c0JsQjZ2VmtXMGFnM01FQQ==",
    verify_certs=False,
    request_timeout=120
)

index_name = "grc-risk-register"

# -----------------------------
# 2️⃣ Check if index exists, create if not
# -----------------------------
try:
    if not es.indices.exists(index=index_name):
        print(f"Index '{index_name}' does not exist. Creating...")
        es.indices.create(index=index_name, ignore=400)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")
except ApiError as e:
    print(f"Error checking/creating index: {e}")

# -----------------------------
# 3️⃣ Sample risk records to insert
# -----------------------------
risk_list = [
    {
        "risk_id": "RISK-001",
        "risk_name": "Unauthorized Access",
        "risk_category": "Security",
        "impact": 5,
        "likelihood": 4,
        "owner": "IT",
        "status": "Open",
        "created_at": datetime.now(timezone.utc).isoformat()
    },
    {
        "risk_id": "RISK-002",
        "risk_name": "Data Loss",
        "risk_category": "Operational",
        "impact": 4,
        "likelihood": 3,
        "owner": "Ops",
        "status": "Open",
        "created_at": datetime.now(timezone.utc).isoformat()
    },
    {
        "risk_id": "RISK-003",
        "risk_name": "Regulatory Non-Compliance",
        "risk_category": "Compliance",
        "impact": 5,
        "likelihood": 2,
        "owner": "Legal",
        "status": "Open",
        "created_at": datetime.now(timezone.utc).isoformat()
    }
]

# -----------------------------
# 4️⃣ Insert documents safely
# -----------------------------
for risk in risk_list:
    try:
        res = es.index(index=index_name, document=risk)
        print(f"Inserted risk '{risk['risk_id']}' successfully. ES ID: {res['_id']}")
    except ApiError as e:
        print(f"Failed to insert risk '{risk['risk_id']}': {e}")

# -----------------------------
# 5️⃣ Verify inserted documents
# -----------------------------
try:
    result = es.search(index=index_name, query={"match_all": {}})
    print(f"\nTotal documents in '{index_name}': {result['hits']['total']['value']}")
except ApiError as e:
    print(f"Error fetching documents: {e}")
