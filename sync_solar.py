import json
import os
import sys
import time
import requests
import urllib3
from datetime import datetime, timezone
from fusion_solar_py.client import FusionSolarClient

# Disable SSL verification for FusionSolar's self-signed cert
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
requests.packages.urllib3.disable_warnings()
_original_request = requests.Session.request
def _no_verify_request(self, method, url, **kwargs):
    kwargs.setdefault('verify', False)
    return _original_request(self, method, url, **kwargs)
requests.Session.request = _no_verify_request

CF_ACCOUNT_ID = os.environ['CF_ACCOUNT_ID']
CF_KV_ID = os.environ['CF_KV_ID']
CF_TOKEN = os.environ['CF_TOKEN']

cf_headers = {'Authorization': f'Bearer {CF_TOKEN}'}


def kv_url(key):
    return (
        f'https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}'
        f'/storage/kv/namespaces/{CF_KV_ID}/values/{key}'
    )


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def status_key(station_id):
    return f'FUSIONSOLAR_SYNC_STATUS_{station_id}'


def read_status(station_id):
    resp = requests.get(kv_url(status_key(station_id)), headers=cf_headers, verify=True)
    if resp.status_code == 404:
        return {}
    resp.raise_for_status()
    raw = resp.text.strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f'Invalid FusionSolar status JSON for station={station_id}: {e}')
    if not isinstance(data, dict):
        raise ValueError(f'FusionSolar status for station={station_id} is not an object')
    return data


def write_status(station_id, updates):
    current = {}
    try:
        current = read_status(station_id)
    except Exception as e:
        print(f'Could not read existing status for station={station_id}: {e}', file=sys.stderr)
    payload = {
        **current,
        **updates,
        'stationId': station_id,
        'updatedAt': now_iso(),
    }
    if not payload.get('registeredAt'):
        payload['registeredAt'] = current.get('registeredAt') or updates.get('registeredAt') or now_iso()
    resp = requests.put(
        kv_url(status_key(station_id)),
        data=json.dumps(payload),
        headers={**cf_headers, 'Content-Type': 'application/json'},
        verify=True,
    )
    resp.raise_for_status()
    return payload


def mark_status(station_id, updates):
    last_error = None
    for attempt in range(3):
        try:
            return write_status(station_id, updates)
        except Exception as e:
            last_error = e
            print(
                f'Failed to update FusionSolar status for station={station_id} '
                f'(attempt {attempt + 1}/3): {e}',
                file=sys.stderr,
            )
            time.sleep(0.5 * (attempt + 1))
    print(
        f'Giving up on FusionSolar status update for station={station_id}: {last_error}',
        file=sys.stderr,
    )
    return None


def _debug_station_identifiers(stations):
    return [
        {
            'stationCode': s.get('stationCode'),
            'dn': s.get('dn') or s.get('stationDn') or s.get('plantDn'),
            'stationName': s.get('stationName') or s.get('name'),
        }
        for s in stations
        if isinstance(s, dict)
    ]


def resolve_plant_id(client, station_id):
    stations = client.get_station_list()
    plant_ids = client.get_plant_ids()

    print(f'FusionSolar debug: SYNC_JOBS stationId={station_id}')
    print(f'FusionSolar debug: get_plant_ids={plant_ids}')
    print(f'FusionSolar debug: get_station_list identifiers={_debug_station_identifiers(stations)}')

    station_id_str = str(station_id)
    plant_id_candidates = [str(pid) for pid in plant_ids if pid is not None]
    if station_id_str in plant_id_candidates:
        return station_id_str

    for station in stations:
        if not isinstance(station, dict):
            continue
        station_code = str(station.get('stationCode') or '')
        if station_code == station_id_str:
            resolved = station.get('dn') or station.get('stationDn') or station.get('plantDn')
            if resolved:
                print(f'FusionSolar debug: mapped stationCode={station_id_str} -> plantId={resolved}')
                return str(resolved)

    if len(plant_id_candidates) == 1:
        resolved = plant_id_candidates[0]
        print(f'FusionSolar debug: fallback single plantId={resolved} for stationId={station_id_str}')
        return resolved

    raise ValueError(
        f'Could not resolve FusionSolar plant id for stationId={station_id_str}; '
        f'plantIds={plant_id_candidates}; stationList={_debug_station_identifiers(stations)}'
    )


# Load sync jobs from KV — guard against all HTTP error codes, not just 404.
resp = requests.get(kv_url('SYNC_JOBS'), headers=cf_headers, verify=True)

if resp.status_code == 404:
    print('No sync jobs registered yet — nothing to do.')
    sys.exit(0)

if not resp.ok:
    # Non-404 failure (e.g. 401 bad token, 403 forbidden, 500 server error).
    # Treat as transient and exit cleanly so GitHub does not mark the run red
    # unless it happens consistently.
    print(
        f'KV read for SYNC_JOBS failed ({resp.status_code}): '
        f'{resp.text[:300]}',
        file=sys.stderr,
    )
    sys.exit(1)

raw_body = resp.text.strip()
if not raw_body:
    print('SYNC_JOBS key exists but is empty — nothing to do.')
    sys.exit(0)

try:
    jobs = json.loads(raw_body)
except json.JSONDecodeError as e:
    # Cloudflare returned something that is not valid JSON.  Log the raw body
    # so we can see whether it was an HTML error page or garbage.
    print(
        f'SYNC_JOBS is not valid JSON: {e}\n'
        f'Raw body (first 300 chars): {raw_body[:300]}',
        file=sys.stderr,
    )
    sys.exit(1)

if not isinstance(jobs, list):
    # Cloudflare API error responses are dicts, not lists.
    print(
        f'SYNC_JOBS parsed to {type(jobs).__name__} instead of list — '
        f'possible Cloudflare API error: {raw_body[:300]}',
        file=sys.stderr,
    )
    sys.exit(1)

print(f'Found {len(jobs)} sync job(s)')

for job in jobs:
    username = job['username']
    password = job['password']
    region = job.get('region', 'uni002eu5')
    station_id = job['stationId']
    kv_key = f'SOLAR_LIVE_{station_id}'
    legacy_kv_key = 'SOLAR_LIVE'

    try:
        mark_status(station_id, {
            'lastAttemptAt': now_iso(),
            'lastStage': 'fetching_kpi',
            'lastErrorAt': None,
            'lastErrorMessage': None,
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        client = FusionSolarClient(username, password, huawei_subdomain=region)
        plant_id = resolve_plant_id(client, station_id)
        mark_status(station_id, {
            'resolvedPlantId': plant_id,
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        kpi = client.get_current_plant_data(plant_id)
        mark_status(station_id, {
            'lastStage': 'kpi_fetched',
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
            'resolvedPlantId': plant_id,
        })

        if not isinstance(kpi, dict) or not kpi:
            raise ValueError('FusionSolar KPI response was empty or malformed')

        # storage_charge_discharge_power: positive = charging, negative = discharging
        batt_raw = kpi.get('storage_charge_discharge_power', 0) or 0

        # inverter_power is the AC generation output from the inverter (kW).
        # radiation_intensity is solar irradiance (W/m²) — not the same thing.
        solar_kw = kpi.get('inverter_power', kpi.get('activePower', kpi.get('active_power', 0))) or 0
        # use_power is the site consumption (load).
        consumption_kw = kpi.get('use_power', kpi.get('inverter_power', 0)) or 0

        # Battery SOC — try every field name FusionSolar uses across firmware versions.
        # Store None (JSON null) when absent so the app knows there is no battery,
        # rather than defaulting to 0 which looks like a dead battery.
        _soc_raw = (
            kpi.get('storage_state_of_charge')
            or kpi.get('batteryStateOfCharge')
            or kpi.get('battery_soc')
            or kpi.get('ess_soc')
        )
        battery_soc = float(_soc_raw) if _soc_raw is not None and float(_soc_raw) > 0 else None

        data_to_send = {
            'solar_power': solar_kw,
            'battery_soc': battery_soc,
            'battery_charge': max(0.0, batt_raw),
            'battery_discharge': max(0.0, -batt_raw),
            'consumption': consumption_kw,
            'grid_export': kpi.get('grid_power', 0) or 0,
            'synced_at': datetime.now(timezone.utc).isoformat(),
            'raw': kpi,
        }

        put_resp = requests.put(
            kv_url(kv_key),
            data=json.dumps(data_to_send),
            headers={**cf_headers, 'Content-Type': 'application/json'},
            verify=True,
        )
        put_resp.raise_for_status()

        status_result = mark_status(station_id, {
            'lastStage': 'live_data_written',
            'lastSuccessAt': now_iso(),
            'lastErrorAt': None,
            'lastErrorMessage': None,
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
            'resolvedPlantId': plant_id,
        })
        if status_result is None:
            raise RuntimeError(f'FusionSolar live data was written but status update failed for station={station_id}')

        try:
            legacy_put_resp = requests.put(
                kv_url(legacy_kv_key),
                data=json.dumps(data_to_send),
                headers={**cf_headers, 'Content-Type': 'application/json'},
                verify=True,
            )
            legacy_put_resp.raise_for_status()
        except Exception as e:
            print(
                f'Legacy SOLAR_LIVE write failed for station={station_id}: {e}',
                file=sys.stderr,
            )

        print(f'Synced station={station_id} ({job.get("stationName", "")}) → {kv_key}')

    except json.JSONDecodeError as e:
        # FusionSolar returned an HTML error page instead of JSON.
        # This usually means the runner IP is rate-limited or temporarily blocked.
        # Print to stderr so GitHub marks the step as failed.
        mark_status(station_id, {
            'lastStage': 'failed',
            'lastAttemptAt': now_iso(),
            'lastErrorAt': now_iso(),
            'lastErrorMessage': f'FusionSolar returned non-JSON: {e}',
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        print(
            f'FusionSolar returned non-JSON for station={station_id} '
            f'(likely rate-limited or IP blocked): {e}',
            file=sys.stderr,
        )

    except Exception as e:
        mark_status(station_id, {
            'lastStage': 'failed',
            'lastAttemptAt': now_iso(),
            'lastErrorAt': now_iso(),
            'lastErrorMessage': str(e),
            'jobId': job.get('jobId'),
            'householdId': job.get('householdId'),
            'userId': job.get('userId'),
            'stationName': job.get('stationName'),
            'region': region,
        })
        print(f'Error syncing station={station_id}: {e}', file=sys.stderr)
