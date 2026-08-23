import os
import json
from datetime import datetime

# Simulated data fetching (In a real scenario, this would query Elasticsearch, IAM providers, or CMDB)
def fetch_users():
    return [
        {"name": "admin", "mfa": False, "last_login": "2026-01-20"},
        {"name": "analyst", "mfa": True, "last_login": "2026-01-22"},
        {"name": "guest", "mfa": False, "last_login": "2025-11-01"},
    ]

def fetch_assets():
    return [
        {"asset_id": "SRV-001", "type": "Server", "owner": "IT", "encrypted": True},
        {"asset_id": "LT-102", "type": "Laptop", "owner": "HR", "encrypted": False},
    ]

print("=== ISO 27001 Automated Control Mapping ===\n")

# A.9 Access Control
print("Checking Control A.9: Access Control (MFA & Inactive Accounts)")
users = fetch_users()
for user in users:
    issues = []
    if not user["mfa"]:
        issues.append("MFA NOT enabled")
    
    last_login = datetime.strptime(user["last_login"], "%Y-%m-%d")
    days_inactive = (datetime.now() - last_login).days
    if days_inactive > 30:
        issues.append(f"Inactive for {days_inactive} days")
        
    if issues:
        print(f"  [FAIL] User: {user['name']} -> {', '.join(issues)}")
    else:
        print(f"  [PASS] User: {user['name']} -> Compliant")
print()

# A.8 Asset Management
print("Checking Control A.8: Asset Management (Encryption Check)")
assets = fetch_assets()
for asset in assets:
    if not asset["encrypted"]:
        print(f"  [FAIL] Asset: {asset['asset_id']} ({asset['type']}) -> Not Encrypted")
    else:
        print(f"  [PASS] Asset: {asset['asset_id']} ({asset['type']}) -> Compliant")
print()

# A.12 Operations Security
print("Checking Control A.12: Operations Security (Log ingestion)")
log_dir = "logs"
if os.path.exists(log_dir) and any(os.scandir(log_dir)):
    print(f"  [PASS] Log directory '{log_dir}' is populated.")
else:
    print(f"  [FAIL] Log directory '{log_dir}' is empty or missing. Evidence of logging is required.")
print("\n=== Check Complete ===")
