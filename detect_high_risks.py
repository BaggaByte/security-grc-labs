from elasticsearch import Elasticsearch
from datetime import datetime, timezone
import uuid

es = Elasticsearch(
    "https://localhost:9200",
    api_key="TWVVczBwc0JzRUxWNF9FZW5DRDg6V1V5U3Y3c0JsQjZ2VmtXMGFnM01FQQ==",
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

