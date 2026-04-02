import requests
import os
import json

USER = os.getenv("HUAWEI_USER")
PASS = os.getenv("HUAWEI_PASS")
REGION = "uni002eu5"

def get_data():
    s = requests.Session()
    s.headers.update({
        "User-Agent": "FusionSolar/6.26.1.1 (iPhone; iOS 17.4.1)",
        "ClientType": "10",
        "v-version": "6.26.1.1",
        "Accept": "application/json",
        "Content-Type": "application/json"
    })

    # This is the specific "Unified Cluster" login path
    login_url = f"https://{REGION}.fusionsolar.huawei.com/rest/app/v1/services/login"
    
    try:
        # 1. Login
        r = s.post(login_url, json={"userName": USER, "password": PASS}, timeout=15)
        
        if r.status_code == 200:
            data = r.json()
            token = r.headers.get("xsrf-token")
            
            if token and data.get("success") != False:
                print("Login Successful!")
                # Extract stationCode from the login response directly
                station_list = data.get("data", {}).get("stationList", [])
                if not station_list:
                    print("Login worked, but no stations found in this account.")
                    return None
                    
                st_code = station_list[0].get("stationCode")

                # 2. Get Real-Time Data (KPI)
                kpi_url = f"https://{REGION}.fusionsolar.huawei.com/rest/app/v1/getStationRealKpi"
                kpi_res = s.post(kpi_url, json={"stationCodes": st_code}, headers={"xsrf-token": token})
                
                return kpi_res.json()
            else:
                print(f"Login logic failed: {data.get('message')}")
        else:
            # If this is still 404, we will try the secondary 'uniflow' path
            print(f"Primary endpoint 404'd. Status: {r.status_code}")
            return None
            
    except Exception as e:
        print(f"Connection Error: {e}")
    return None

def push_to_kv(data):
    cf_url = f"https://api.cloudflare.com/client/v4/accounts/{os.getenv('CF_ACCOUNT_ID')}/storage/kv/namespaces/{os.getenv('CF_KV_ID')}/values/SOLAR_LIVE"
    h = {"Authorization": f"Bearer {os.getenv('CF_TOKEN')}", "Content-Type": "text/plain"}
    requests.put(cf_url, headers=h, data=json.dumps(data))

if __name__ == "__main__":
    result = get_data()
    if result:
        push_to_kv(result)
        print("Success: Data pushed to Cloudflare.")
