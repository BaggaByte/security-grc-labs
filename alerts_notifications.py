# alerts_notifications.py
from elasticsearch import Elasticsearch, ApiError
from datetime import datetime, timezone

# -----------------------------
# 1️⃣ Connect to Elasticsearch
# -----------------------------
es = Elasticsearch(
    "https://localhost:9200",
    api_key="TWVVczBwc0JzRUxWNF9FZW5DRDg6V1V5U3Y3c0JsQjZ2VmtXMGFnM01FQQ==",
    verify_certs=False,
    request_timeout=120
)

# -----------------------------
# 2️⃣ Define indices
# -----------------------------
risks_index = "grc-risk-register"
evidence_index = "grc-evidence"

# -----------------------------
# 3️⃣ Fetch all risks
# -----------------------------
try:
    risks_result = es.search(
        index=risks_index,
        query={"match_all": {}},
        size=1000  # adjust if you have more risks
    )
    risks = risks_result['hits']['hits']
except ApiError as e:
    print(f"Error fetching risks: {e}")
    risks = []

# -----------------------------
# 4️⃣ Check for missing evidence
# -----------------------------
for risk_doc in risks:
    risk = risk_doc['_source']
    risk_id = risk.get('risk_id', 'Unknown')
    risk_name = risk.get('risk_name', 'Unnamed')
    impact = risk.get('impact', 0)
    
    # Fetch evidence linked to this risk
    try:
        evidence_result = es.search(
            index=evidence_index,
            query={"match": {"risk_id": risk_id}},
            size=100
        )
        evidence_list = evidence_result['hits']['hits']
    except ApiError as e:
        print(f"Error fetching evidence for risk {risk_id}: {e}")
        evidence_list = []

    # Alert condition: High impact & no evidence
    if impact >= 4 and not evidence_list:
        print(f"⚠️ ALERT: High-impact risk '{risk_name}' (ID: {risk_id}) has NO evidence!")

    # Optional: print all evidence found
    if evidence_list:
        print(f"✅ Risk '{risk_name}' (ID: {risk_id}) has {len(evidence_list)} evidence items.")

# -----------------------------
# 5️⃣ Summary
# -----------------------------
print(f"\nTotal risks checked: {len(risks)}")
