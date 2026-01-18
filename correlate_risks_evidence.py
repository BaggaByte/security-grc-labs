from elasticsearch import Elasticsearch

es = Elasticsearch(
    "https://localhost:9200",
    api_key="TWVVczBwc0JzRUxWNF9FZW5DRDg6V1V5U3Y3c0JsQjZ2VmtXMGFnM01FQQ==",
    verify_certs=False,
    request_timeout=120
)
# Fetch all risks
risks = es.search(index="grc-risk-register", query={"match_all": {}})['hits']['hits']

# Fetch all evidence
evidence = es.search(index="grc-evidence", query={"match_all": {}})['hits']['hits']
# Create a mapping: risk_id -> list of evidence
risk_evidence_map = {}
for ev in evidence:
    rid = ev['_source']['risk_id']
    if rid not in risk_evidence_map:
        risk_evidence_map[rid] = []
    risk_evidence_map[rid].append(ev['_source'])

# Print report
for r in risks:
    risk_id = r['_source']['risk_id']
    risk_name = r['_source']['risk_name']
    linked_evidence = risk_evidence_map.get(risk_id, [])
    print(f"\nRisk: {risk_id} - {risk_name}")
    print(f"Linked Evidence Count: {len(linked_evidence)}")
    for ev in linked_evidence:
        print(f"  - Evidence: {ev['evidence_id']}, Type: {ev['type']}, Uploaded: {ev['uploaded_at']}")
