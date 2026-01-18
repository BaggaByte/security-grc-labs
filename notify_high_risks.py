from elasticsearch import Elasticsearch, ApiError
from datetime import datetime
import os

# -----------------------------
# 1️⃣ Elasticsearch connection
# -----------------------------
es = Elasticsearch(
    "https://localhost:9200",
    api_key="TWVVczBwc0JzRUxWNF9FZW5DRDg6V1V5U3Y3c0JsQjZ2VmtXMGFnM01FQQ==",  # replace with your API key
    verify_certs=False,
    request_timeout=120
)

risk_index = "grc-risk-register"
log_file = "high_risk_alerts.log"

# -----------------------------
# 2️⃣ Query high-risk items
# -----------------------------
query = {
    "bool": {
        "should": [
            {"range": {"impact": {"gte": 4}}},
            {"range": {"likelihood": {"gte": 4}}}
        ]
    }
}

response = es.search(index=risk_index, query=query)


try:
    response = es.search(index=risk_index, query=query)
    hits = response['hits']['hits']

    if not hits:
        print("No high-risk items found.")
    else:
        print(f"Found {len(hits)} high-risk items. Logging alerts...")

        with open(log_file, "a") as f:
            for hit in hits:
                risk = hit['_source']
                alert_msg = (
                    f"{datetime.now()} | High-Risk Alert: "
                    f"Risk ID: {risk['risk_id']}, "
                    f"Name: {risk['risk_name']}, "
                    f"Category: {risk['risk_category']}, "
                    f"Impact: {risk['impact']}, "
                    f"Likelihood: {risk['likelihood']}, "
                    f"Owner: {risk['owner']}, "
                    f"Status: {risk['status']}\n"
                )
                f.write(alert_msg)
                print(alert_msg.strip())

except ApiError as e:
    print(f"Elasticsearch API error: {e}")
except Exception as e:
    print(f"Error: {e}")
