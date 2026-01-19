from elasticsearch import Elasticsearch
from datetime import datetime

# Elasticsearch connection
es = Elasticsearch(
    "https://localhost:9200",
    basic_auth=("elastic", "b4bL7JQtp=xkhaIPc3t+"),
    verify_certs=False
)

# GRC Evidence Document
document = {
    "@timestamp": datetime.utcnow(),
    "control_id": "ISO27001-A.9.2.3",
    "control_name": "User Access Review",
    "risk": "Unauthorized access",
    "evidence_type": "Automated access review",
    "status": "Compliant",
    "owner": "IT Security",
    "review_date": "2026-01-15",
    "source": "python-automation"
}

# Index data
response = es.index(index="grc-evidence-logs", document=document)

print("Evidence indexed:", response["result"])
