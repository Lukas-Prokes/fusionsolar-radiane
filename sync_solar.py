import requests
import os
import json

USER = os.getenv("HUAWEI_USER")
PASS = os.getenv("HUAWEI_PASS")
REGION = "uni002eu5"

def get_data():
    s = requests.Session()
    # We are now pretending to be a Desktop Chrome browser
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Content-Type": "application/json",
        "X-Requested-With": "XMLHttpRequest"
    })

    # The Web Portal Login path
    login_url = f"https://{REGION}.fusionsolar.huawei.com/rest/openapi/common/login"
    
    try:
        # Step 1: Login
        payload = {"userName": USER, "password": PASS}
        r = s.post(login_url, json=payload, timeout=20)
        
        print(f"Server Status: {r.status_code}")
        
        # Check if login was successful
        if r.status_code == 200:
            token = r.headers.get("xsrf-token")
            # In the browser flow, the token might also be in a cookie
            if not token:
                token = s.cookies.get("XSRF-TOKEN")

            if token:
                print("Login Successful! Handshake complete.")
                
                # Step 2: Get Station Code
                list_url = f"https://{REGION}.fusionsolar.huawei.com/rest/openapi/pvms/v1/station/get-station-list"
                list_res = s.post(list_url, json={}, headers={"xsrf-token": token})
                
                stations = list_res.json().get("data", {}).get("list", [])
                if stations:
                    st_code = stations[0].get("stationCode")
                    
                    # Step 3: Get the Battery/KPI data
                    kpi_url = f"https://{REGION}.fusionsolar.huawei.com/rest/openapi/pvms/v1/station/get-real-kpi"
                    kpi_res = s.post(kpi_url, json={"stationCodes": st_code}, headers={"xsrf-token": token})
                    return kpi_res.json()
            else:
                print(f"Login failed: Response was {r.text}")
        else:
            print(f"Network Error: {r.status_code}")
            
    except Exception as e:
        print(f"Script Error: {e}")
    return None

def push_to_kv(data):
    url = f"https://api.cloudflare.com/client/v4/accounts/{os.getenv('CF_ACCOUNT_ID')}/storage/kv/namespaces/{os.getenv('CF_KV_ID')}/values/SOLAR_LIVE"
    h = {"Authorization": f"Bearer {os.getenv('CF_TOKEN')}", "Content-Type": "text/plain"}
    requests.put(url, headers=h, data=json.dumps(data))

if __name__ == "__main__":
    result = get_data()
    if result:
        push_to_kv(result)
        print("Data successfully bridged to Cloudflare.")
