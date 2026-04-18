import os
import sys
import json
import requests
import urllib3
from fusion_solar_py.client import FusionSolarClient

# Disable SSL verification for FusionSolar's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()
_original_request = requests.Session.request
def _no_verify_request(self, method, url, **kwargs):
    kwargs.setdefault('verify', False)
    return _original_request(self, method, url, **kwargs)
requests.Session.request = _no_verify_request

HUAWEI_USER = os.environ['HUAWEI_USER']
HUAWEI_PASS = os.environ['HUAWEI_PASS']
HUAWEI_REGION = os.environ.get('HUAWEI_REGION', 'uni002eu5')
JOB_ID = os.environ['JOB_ID']

CF_ACCOUNT_ID = os.environ['CF_ACCOUNT_ID']
CF_KV_ID = os.environ['CF_KV_ID']
CF_TOKEN = os.environ['CF_TOKEN']


def kv_url(key):
    return (
        f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}'
        f'/storage/kv/namespaces/{CF_KV_ID}/values/{key}'
    )


def write_kv(key, payload):
    requests.put(
        kv_url(key),
        data=json.dumps(payload),
        headers={'Authorization': f'Bearer {CF_TOKEN}', 'Content-Type': 'application/json'},
        verify=True,  # Cloudflare has a valid cert
    ).raise_for_status()


def delete_creds():
    try:
        requests.delete(
            kv_url(f'DISCOVER_CREDS_{JOB_ID}'),
            headers={'Authorization': f'Bearer {CF_TOKEN}'},
            verify=True,
        )
    except Exception:
        pass


try:
    client = FusionSolarClient(HUAWEI_USER, HUAWEI_PASS, huawei_subdomain=HUAWEI_REGION)
    stations = client.get_station_list()

    plants = [
        {
            'id': s['stationCode'],
            'name': s.get('stationName', 'Unknown Plant'),
            'capacity': s.get('capacity', None),
            'location': s.get('stationAddr', s.get('address', None)),
        }
        for s in stations
        if s.get('stationCode')
    ]

    write_kv(f'PLANT_DISCOVERY_{JOB_ID}', {'success': True, 'plants': plants})
    print(f'Stored {len(plants)} plants for job {JOB_ID}')

except Exception as e:
    error_msg = str(e)
    print(f'ERROR: {error_msg}', file=sys.stderr)
    try:
        write_kv(f'PLANT_DISCOVERY_{JOB_ID}', {'success': False, 'error': error_msg})
    except Exception as kv_err:
        print(f'ERROR writing failure to KV: {kv_err}', file=sys.stderr)
    sys.exit(1)

finally:
    delete_creds()
