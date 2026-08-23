import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from datetime import datetime, timezone
import uuid

load_dotenv()

es = Elasticsearch(
    os.getenv("ES_HOST", "https://localhost:9200"),
    api_key=os.getenv("ES_API_KEY"),
    verify_certs=False
)

RISK_INDEX = "grc-risk-register"
ALERT_INDEX = "grc-alerts"

def detect_high_risks():
    query_body = {
        "bool": {
            "must": [
                {"range": {"impact": {"gte": 4}}},
                {"range": {"likelihood": {"gte": 4}}}
            ]
        }
    }

    risks = es.search(
        index=RISK_INDEX,
        query=query_body,
        size=100
    )

    for hit in risks["hits"]["hits"]:
        risk = hit["_source"]

        alert = {
            "alert_id": str(uuid.uuid4()),
            "risk_id": risk["risk_id"],
            "alert_type": "HIGH_RISK",
            "severity": "CRITICAL",
            "message": f"High risk detected: {risk['risk_name']}",
            "detected_at": datetime.now(timezone.utc).isoformat()
        }

        es.index(index=ALERT_INDEX, document=alert)
        print(f" Alert created for {risk['risk_id']}")

if __name__ == "__main__":
    detect_high_risks()

