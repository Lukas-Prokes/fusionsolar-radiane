import os
import json
import requests
from fusion_solar_py.client import FusionSolarClient

def run_sync():
    # 1. Setup credentials
    user = os.getenv("HUAWEI_USER")
    password = os.getenv("HUAWEI_PASS")
    # This is the secret sauce for Sweden/Värmdö
    subdomain = "uni002eu5" 

    try:
        # 2. Login using the specialized client
        print(f"Connecting to {subdomain}...")
        client = FusionSolarClient(user, password, huawei_subdomain=subdomain)
        
        # 3. Get all power/battery stats
        stats = client.get_power_status()
        
        # We convert the object to a dictionary so we can send it to Cloudflare
        data_to_send = {
            "current_power": stats.current_power,
            "total_yield": stats.total_yield,
            "daily_yield": stats.daily_yield,
            "battery_soc": getattr(stats, 'battery_soc', 'N/A'), # Specifically for your battery
            "battery_power": getattr(stats, 'battery_power', 'N/A')
        }

        # 4. Push to Cloudflare KV
        cf_url = f"https://api.cloudflare.com/client/v4/accounts/{os.getenv('CF_ACCOUNT_ID')}/storage/kv/namespaces/{os.getenv('CF_KV_ID')}/values/SOLAR_LIVE"
        headers = {
            "Authorization": f"Bearer {os.getenv('CF_TOKEN')}",
            "Content-Type": "text/plain"
        }
        
        response = requests.put(cf_url, headers=headers, data=json.dumps(data_to_send))
        
        if response.status_code == 200:
            print("Successfully bridged data to Cloudflare!")
        else:
            print(f"Cloudflare Error: {response.text}")

    except Exception as e:
        print(f"Failed to sync: {str(e)}")

if __name__ == "__main__":
    run_sync()
