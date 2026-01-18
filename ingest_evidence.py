from elasticsearch import Elasticsearch, ApiError
from datetime import datetime, timezone

# -----------------------------
# 1️⃣ Connect to Elasticsearch using API Key
# -----------------------------
es = Elasticsearch(
    "https://localhost:9200",
    api_key=("MeUs0psBsELV4_EenCD8", "WUySv7sBlB6vVkW0ag3MEA"),
    verify_certs=False,
    request_timeout=120
)

index_name = "grc-evidence"

# -----------------------------
# 2️⃣ Check if index exists
# -----------------------------
try:
    if not es.indices.exists(index=index_name):
        print(f"Index '{index_name}' does not exist. Creating...")
        es.indices.create(index=index_name)
        print(f"Index '{index_name}' created successfully.")
    else:
        print(f"Index '{index_name}' already exists.")
except ApiError as e:
    print(f"Error checking/creating index: {e}")

# -----------------------------
# 3️⃣ Prepare evidence documents
# -----------------------------
evidence_list = [
    {
        "evidence_id": "EV-001",
        "risk_id": "RISK-001",
        "control_id": "ISO27001-A.9.2.3",
        "type": "Access review report",
        "description": "Quarterly user access review",
        "location": "SharePoint",
        "uploaded_at": datetime.now(timezone.utc).isoformat()
    },
    {
        "evidence_id": "EV-002",
        "risk_id": "RISK-002",
        "control_id": "ISO27001-A.10.1.1",
        "type": "Patch management report",
        "description": "Monthly patch verification",
        "location": "Internal Server",
        "uploaded_at": datetime.now(timezone.utc).isoformat()
    }
]

# -----------------------------
# 4️⃣ Insert documents safely
# -----------------------------
for evidence in evidence_list:
    try:
        res = es.index(index=index_name, document=evidence)
        print(f"Inserted evidence '{evidence['evidence_id']}' successfully. ES ID: {res['_id']}")
    except ApiError as e:
        print(f"Failed to insert evidence '{evidence['evidence_id']}': {e}")

# -----------------------------
# 5️⃣ Verify inserted documents
# -----------------------------
try:
    result = es.search(index=index_name, query={"match_all": {}})
    print(f"\nTotal documents in '{index_name}': {result['hits']['total']['value']}")
except ApiError as e:
    print(f"Error fetching documents: {e}")
