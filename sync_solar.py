import os
import json
import requests
from fusion_solar_py.client import FusionSolarClient

def run_sync():
    user = os.getenv("HUAWEI_USER")
    password = os.getenv("HUAWEI_PASS")
    subdomain = "uni002eu5" 

    try:
        print(f"Connecting to {subdomain}...")
        client = FusionSolarClient(user, password, huawei_subdomain=subdomain)
        
        # 1. Get the actual Plant ID (Huawei calls it plant_id or stationCode)
        plant_ids = client.get_plant_ids()
        if not plant_ids:
            print("Login worked, but no plants found.")
            return
        
        target_plant = plant_ids[0]
        print(f"Targeting Plant: {target_plant}")

        # 2. Get the RAW data dictionary (This contains the deep stats)
        plant_stats = client.get_plant_stats(target_plant)
        
        # 3. Use the library's helper to extract the LATEST valid measurements
        last_values = client.get_last_plant_data(plant_stats)

        # 4. Map the data using the labels Huawei uses in 2026
        # productPower = Solar, chargePower = Battery In, dischargePower = Battery Out
        data_to_send = {
            "solar_power": last_values.get('productPower', {}).get('value', 0),
            "battery_charge": last_values.get('chargePower', {}).get('value', 0),
            "battery_discharge": last_values.get('dischargePower', {}).get('value', 0),
            "consumption": last_values.get('usePower', {}).get('value', 0),
            "grid_export": last_values.get('onGridPower', {}).get('value', 0)
        }

        print(f"REAL DATA EXTRACTED: {data_to_send}")

        # 5. Push to Cloudflare
        cf_url = f"https://api.cloudflare.com/client/v4/accounts/{os.getenv('CF_ACCOUNT_ID')}/storage/kv/namespaces/{os.getenv('CF_KV_ID')}/values/SOLAR_LIVE"
        headers = {"Authorization": f"Bearer {os.getenv('CF_TOKEN')}", "Content-Type": "text/plain"}
        
        response = requests.put(cf_url, headers=headers, data=json.dumps(data_to_send))
        print("Success! Cloudflare updated with REAL values." if response.status_code == 200 else f"CF Error: {response.text}")

    except Exception as e:
        print(f"Sync Failed: {str(e)}")

if __name__ == "__main__":
    run_sync()
