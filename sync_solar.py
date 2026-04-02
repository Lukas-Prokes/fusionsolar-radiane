import requests
import os
import json

# Config
USER = os.getenv("HUAWEI_USER")
PASS = os.getenv("HUAWEI_PASS")
REGION = "region01eu5" # Change if your login URL is different

def get_data():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "FusionSolar/6.26.1.1 (iPhone; iOS 17.4.1)",
        "ClientType": "10"
    })

    # 1. Login
    login_url = f"https://{REGION}.fusionsolar.huawei.com/rest/app/v1/services/login"
    r = s.post(login_url, json={"userName": USER, "password": PASS})
    token = r.headers.get("xsrf-token")
    
    if not token:
        print("Login failed. Check credentials.")
        return None

    # 2. Get Station Code
    # The first station in your list
    station_data = r.json().get("data", {}).get("stationList", [{}])[0]
    station_code = station_data.get("stationCode")

    # 3. Get Real-Time KPI (Battery, Power, etc)
    kpi_url = f"https://{REGION}.fusionsolar.huawei.com/rest/app/v1/getStationRealKpi"
    kpi_res = s.post(kpi_url, json={"stationCodes": station_code}, headers={"xsrf-token": token})
    
    return kpi_res.json()

def push_to_cloudflare(data):
    # This pushes the result directly into your Worker's KV storage
    url = f"https://api.cloudflare.com/client/v4/accounts/{os.getenv('CF_ACCOUNT_ID')}/storage/kv/namespaces/{os.getenv('CF_KV_ID')}/values/SOLAR_LIVE"
    headers = {"Authorization": f"Bearer {os.getenv('CF_TOKEN')}", "Content-Type": "text/plain"}
    requests.put(url, headers=headers, data=json.dumps(data))

if __name__ == "__main__":
    result = get_data()
    if result:
        push_to_cloudflare(result)
        print("Success: Data synced to Cloudflare KV.")
