import requests
import json
import time

base_url = "http://localhost:18102"
api_key = "test-api-key-123"

def get_sessions():
    resp = requests.get(f"{base_url}/api/v1/pipe/session/", headers={"X-API-Key": api_key})
    print(f"Status: {resp.status_code}")
    print(f"Body: {json.dumps(resp.json(), indent=2)}")

if __name__ == "__main__":
    get_sessions()
