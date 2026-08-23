import os
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from datetime import datetime

load_dotenv()

# Elasticsearch connection
es = Elasticsearch(
    os.getenv("ES_HOST", "https://localhost:9200"),
    basic_auth=(os.getenv("ES_USER", "elastic"), os.getenv("ES_PASSWORD", "")),
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
response = es.index(index="grc-evidence", document=document)

print("Evidence indexed:", response["result"])
