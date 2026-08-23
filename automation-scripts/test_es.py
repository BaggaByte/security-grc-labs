import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from datetime import datetime

load_dotenv()

es = Elasticsearch(
    os.getenv("ES_HOST", "https://localhost:9200"),
    basic_auth=(os.getenv("ES_USER", "elastic"), os.getenv("ES_PASSWORD", "")),
    verify_certs=False,
    request_timeout=60
)

print("Connected:")
print(es.info())

doc = {
    "risk_id": "TEST-001",
    "risk_name": "Test risk",
    "risk_category": "Test",
    "impact": 1,
    "likelihood": 1,
    "owner": "Tester",
    "status": "Open",
    "created_at": datetime.utcnow()
}

res = es.index(index="grc-risk-register", document=doc)
print("Indexed:", res)
