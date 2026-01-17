# ISO 27001 A.9 Access Control – Sample Automation

users = [
    {"name": "admin", "mfa": False},
    {"name": "analyst", "mfa": True},
]

print("ISO 27001 Control A.9 – MFA Check\n")

for user in users:
    if not user["mfa"]:
        print(f"[FAIL] {user['name']} → MFA NOT enabled")
    else:
        print(f"[PASS] {user['name']} → MFA enabled")
