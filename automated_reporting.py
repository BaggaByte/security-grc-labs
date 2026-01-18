# automated_reporting.py
from elasticsearch import Elasticsearch, ApiError
import pandas as pd
from datetime import datetime

# -----------------------------
# 1️⃣ Connect to Elasticsearch
# -----------------------------
es = Elasticsearch(
    "https://localhost:9200",
    api_key="TWVVczBwc0JzRUxWNF9FZW5DRDg6V1V5U3Y3c0JsQjZ2VmtXMGFnM01FQQ==",
    verify_certs=False,
    request_timeout=120
)

risks_index = "grc-risk-register"
evidence_index = "grc-evidence"

# -----------------------------
# 2️⃣ Fetch all risks
# -----------------------------
try:
    risks_result = es.search(
        index=risks_index,
        query={"match_all": {}},
        size=1000
    )
    risks = [r['_source'] for r in risks_result['hits']['hits']]
except ApiError as e:
    print(f"Error fetching risks: {e}")
    risks = []

# -----------------------------
# 3️⃣ Fetch all evidence
# -----------------------------
try:
    evidence_result = es.search(
        index=evidence_index,
        query={"match_all": {}},
        size=2000
    )
    evidence = [e['_source'] for e in evidence_result['hits']['hits']]
except ApiError as e:
    print(f"Error fetching evidence: {e}")
    evidence = []

# -----------------------------
# 4️⃣ Merge risks with evidence count
# -----------------------------
df_risks = pd.DataFrame(risks)
df_evidence = pd.DataFrame(evidence)

# Count evidence per risk
evidence_count = df_evidence.groupby('risk_id').size().reset_index(name='evidence_count')
df_report = df_risks.merge(evidence_count, how='left', left_on='risk_id', right_on='risk_id')
df_report['evidence_count'] = df_report['evidence_count'].fillna(0).astype(int)

# -----------------------------
# 5️⃣ Highlight high-risk without evidence
# -----------------------------
df_report['alert'] = df_report.apply(
    lambda row: 'HIGH-IMPACT MISSING EVIDENCE' if row['impact'] >= 4 and row['evidence_count'] == 0 else '',
    axis=1
)

# -----------------------------
# 6️⃣ Save to CSV/Excel
# -----------------------------
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
df_report.to_csv(f"grc_report_{timestamp}.csv", index=False)
df_report.to_excel(f"grc_report_{timestamp}.xlsx", index=False)

print(f"Report generated: grc_report_{timestamp}.csv & .xlsx")
