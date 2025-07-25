import csv
import time
import logging
import os
import sys
import requests
import threading
from requests_oauthlib import OAuth1Session
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# NOTE: existing claim check is currently DISABLED

load_dotenv()

CONSUMER_KEY = os.getenv("CONSUMER_KEY")
CONSUMER_SECRET = os.getenv("CONSUMER_SECRET")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
ACCESS_SECRET = os.getenv("ACCESS_SECRET")

MAX_WORKERS = 6
TIME_SLEEP = 5

# Global pause event for lag handling
pause_event = threading.Event()
PAUSE_DURATION = 90

API_BASE = "https://www.wikidata.org/w/api.php"
PROPERTY_ID = "P13591"
INPUT_FILE = "lux_uris.csv"
SUCCESS_FILE = "lux_upload_success.csv"
FAILURE_FILE = "lux_upload_failures.csv"
LOG_FILE = "lux_upload.log"
REDIRECT_FILE = "wikidata_redirects.csv"


logging.basicConfig(
    filename=LOG_FILE,
    filemode="a",
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO
)

session = OAuth1Session(
    CONSUMER_KEY,
    client_secret=CONSUMER_SECRET,
    resource_owner_key=ACCESS_TOKEN,
    resource_owner_secret=ACCESS_SECRET
)

def extract_lux_id(uri):
    if "data/" in uri:
        return uri.split("data/", 1)[1]
    elif not uri.startswith('/'):
        return uri
    else:
        raise ValueError(f"Invalid LUX URI format: {uri}")

def get_csrf_token():
    r = session.get(API_BASE, params={"action": "query", "meta": "tokens", "format": "json"})
    r.raise_for_status()
    return r.json()["query"]["tokens"]["csrftoken"]


def handle_maxlag_error(response):
    if 'error' in response and response['error'].get('code') == 'maxlag':
        lag = float(response['error'].get('lag', 5))
        wait_time = min(60, max(5, lag * 2))  # wait between 5 and 60 sec
        logging.warning(f"Maxlag detected ({lag:.2f}s). Sleeping for {wait_time:.1f}s before retry.")
        time.sleep(wait_time)
        return True
    return False

def add_lux_uri(qid, lux_id, csrf_token, max_retries=3):
    data = {
        "action": "wbcreateclaim",
        "entity": qid,
        "snaktype": "value",
        "property": PROPERTY_ID,
        "value": f'"{lux_id}"',
        "format": "json",
        "token": csrf_token,
        "maxlag": 5,
        "bot": 1
    }

    for attempt in range(1, max_retries + 1):
        if pause_event.is_set():
            logging.info(f"[{qid}] Paused globally due to maxlag. Sleeping {PAUSE_DURATION}s")
            time.sleep(PAUSE_DURATION)
            pause_event.clear()

        try:
            r = session.post(API_BASE, data=data, timeout=10)
            r.raise_for_status()
            result = r.json()

            if handle_maxlag_error(result):
                logging.warning(f"[{qid}] Maxlag retry {attempt}/{max_retries}")
                pause_event.set()  # trigger global backoff
                continue

            if "claim" in result:
                return result

            logging.warning(f"No 'claim' in response for {qid}. Full response: {result}")
            return None

        except requests.exceptions.RequestException as e:
            logging.warning(f"[{qid}] RequestException (attempt {attempt}): {e}")
            time.sleep(TIME_SLEEP)

    logging.warning(f"Max retries exceeded for {qid}")
    return None

def check_redirect(qid, max_retries=3):
    for attempt in range(1, max_retries + 1):
        try:
            logging.info(f"Checking redirect status for {qid} (attempt {attempt})")
            response = session.get(API_BASE, params={
                "action": "wbgetentities",
                "ids": qid,
                "format": "json"
            }, timeout=10)
            response.raise_for_status()
            data = response.json()
            if "redirects" in data:
                for redirect in data["redirects"]:
                    if redirect.get("from") == qid:
                        return True, redirect.get("to")
            return False, None
        except requests.exceptions.RequestException as e:
            logging.warning(f"Redirect check failed for {qid} (attempt {attempt}): {e}")
            time.sleep(2 * attempt)  # simple exponential backoff
    logging.error(f"Redirect check permanently failed for {qid} after {max_retries} attempts.")
    return False, None




# def batch_get_existing_lux_ids(qids):
#     existing = {}
#     BATCH_SIZE = 50

#     for i in tqdm(range(0, len(qids), BATCH_SIZE), desc="Fetching existing claims", file=sys.stdout):
#         batch = qids[i:i + BATCH_SIZE]
#         try:
#             r = session.get(API_BASE, params={
#                 "action": "wbgetentities",
#                 "ids": "|".join(batch),
#                 "props": "claims",
#                 "format": "json"
#             }, timeout=15)
#             r.raise_for_status()
#             data = r.json().get("entities", {})
#         except requests.exceptions.RequestException as e:
#             logging.warning(f"Failed to fetch claims for batch {batch}: {e}")
#             for q in batch:
#                 existing[q] = None
#             continue

#         for qid in batch:
#             claims = data.get(qid, {}).get("claims", {}).get(PROPERTY_ID, [])
#             lux_values = []
#             for claim in claims:
#                 mainsnak = claim.get("mainsnak", {})
#                 datavalue = mainsnak.get("datavalue", {})
#                 value = datavalue.get("value")
#                 if isinstance(value, str):
#                     lux_values.append((claim["id"], value))
#             existing[qid] = lux_values
#     return existing

def process_record(qid, uri, lux_id, csrf_token):
    time.sleep(TIME_SLEEP)
    try:
        result = add_lux_uri(qid, lux_id, csrf_token)
        if result:
            logging.info(f"Added LUX URI to {qid}")
            return ("success", qid, lux_id, "added")
        else:
            logging.warning(f"No claim created for {qid}")
            return ("fail", qid, lux_id, "No claim in response")
    except Exception as e:
        logging.error(f"Error adding LUX URI to {qid}: {e}")
        return ("fail", qid, lux_id, f"Exception: {type(e).__name__}")


# === Load already processed QIDs ===
processed_qids = set()
try:
    with open(SUCCESS_FILE, newline="") as f:
        rdr = csv.reader(f)
        for row in rdr:
            if row and row[0].startswith("Q"):
                processed_qids.add(row[0])
except FileNotFoundError:
    print(f"Success file not found: {SUCCESS_FILE}, exiting...")
    sys.exit(1)

# === Load input CSV ===
input_rows = []
all_qids = []
with open(INPUT_FILE, newline="") as infile:
    reader = csv.reader(infile)
    for row in reader:
        if row and row[0].startswith("Q"):
            qid, uri = row[0], row[1]
            if qid not in processed_qids:
                input_rows.append((qid, uri))
                all_qids.append(qid)

print(f"✅ Loaded {len(input_rows)} records to process...")

# === Fetch existing claims in batch ===
#qid_to_existing_claims = batch_get_existing_lux_ids(all_qids)
qid_to_existing_claims = {qid: [] for qid in all_qids}

# === Filter to those needing addition ===
to_add = []
for qid, uri in input_rows:
    try:
        lux_id = extract_lux_id(uri)
    except ValueError as e:
        logging.warning(str(e))
        to_add.append(("fail", qid, "Invalid", "Invalid LUX format"))
        continue

    claims = qid_to_existing_claims.get(qid)
    if claims is None:
        to_add.append(("fail", qid, lux_id, "Claim fetch failed"))
    elif claims:
        to_add.append(("success", qid, lux_id, "already exists"))
    else:
        to_add.append(("pending", qid, uri, lux_id))  # needs writing

csrf_token = get_csrf_token()

# === Open writers ===
with open(SUCCESS_FILE, "a", newline="", encoding="utf-8") as success_f, \
     open(FAILURE_FILE, "a", newline="", encoding="utf-8") as fail_f, \
     open(REDIRECT_FILE, "a", newline="", encoding="utf-8") as redirect_f:


    success_writer = csv.writer(success_f)
    fail_writer = csv.writer(fail_f)
    redirect_writer = csv.writer(redirect_f)

    tasks = []
    for status, qid, uri, lux_id in to_add:
        if status == "pending":
            is_redir, target = check_redirect(qid)
            if is_redir:
                redirect_writer.writerow([qid, target])
                logging.info(f"[{qid}] is a redirect to [{target}], skipping.")
                continue
            tasks.append((qid, uri, lux_id))
        elif status == "fail":
            fail_writer.writerow([qid, lux_id, "Invalid LUX format"])
        elif status == "success":
            success_writer.writerow([qid, lux_id, "already exists"])


    print(f"🧵 Starting threaded upload of {len(tasks)} records...")

    with ThreadPoolExecutor(MAX_WORKERS) as executor:
        futures = {
            executor.submit(process_record, qid, uri, lux_id, csrf_token): (qid, lux_id)
            for qid, uri, lux_id in tasks
        }

        for future in tqdm(as_completed(futures), total=len(futures), desc="Adding LUX IDs", file=sys.stdout):
            result_type, qid, lux_id, msg = future.result()
            if result_type == "success":
                success_writer.writerow([qid, lux_id, msg])
            else:
                fail_writer.writerow([qid, lux_id, msg])
