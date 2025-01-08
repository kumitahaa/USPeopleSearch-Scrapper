import pandas as pd
import time, requests, random, sys
from collections import deque
from datetime import datetime
import shutil

# Constants
TCPA_API_URL = "https://tcpa.api.uspeoplesearch.net/tcpa/v1"
PI_DEFAULT = ".".join(str(random.randint(0, 255)) for _ in range(4))
TIMEOUT = 10
BACKUP_FILE = "data_old.txt"

# Globals for counters
successful_tcpa_api = 0
failed_tcpa_api = 0

def fetch_tcpa_data(phone, pi=PI_DEFAULT):
    """Fetch TCPA data for a given phone number."""
    global successful_tcpa_api, failed_tcpa_api
    params = {'x': phone, 'pi': pi}
    default_tcpa = {
        "Phone": phone, 
        "National DNC": "ZZZ", 
        "State DNC": "ZZZ", 
        "Blacklisted": "ZZZ",
        "State": "ZZZ"
    }
    
    try:
        response = requests.get(TCPA_API_URL, params=params, timeout=TIMEOUT)
        print(f"  [TCPA Response]: {response.text}")  # Print the raw response
        response.raise_for_status()
        data = response.json()
        successful_tcpa_api += 1
        return {
            "Phone": data.get("phone", phone),
            "National DNC": data.get("ndnc", "ZZZ"),
            "State DNC": data.get("sdnc", "ZZZ"),
            "Blacklisted": data.get("listed", "ZZZ"),
            "State": data.get("state", "ZZZ")
        }
    except (requests.RequestException, ValueError) as e:
        print(f"  [TCPA Error] Failed to fetch TCPA data for {phone}: {e}")
        failed_tcpa_api += 1
        return default_tcpa

def backup_file(original, backup):
    """Create a backup of the original file."""
    shutil.copy(original, backup)

def load_phone_numbers(file):
    """Load phone numbers into a deque."""
    try:
        with open(file, "r", encoding="utf-8") as f:
            return deque([line.strip() for line in f if line.strip()])
    except FileNotFoundError:
        print(f"Error: Input file '{file}' not found.")
        sys.exit(1)

def save_remaining_data(file, phone_numbers):
    """Save unprocessed phone numbers back to the input file."""
    with open(file, "w", encoding="utf-8") as f:
        f.writelines(f"{num}\n" for num in phone_numbers)

def init():
    """Initial setup: Backup file and prepare output structure."""
    global phone_numbers, total_numbers, output_file, df

    input_file = "data.txt"
    backup_file_name = BACKUP_FILE
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{timestamp}_tcpa.csv"

    # Backup original file
    backup_file(input_file, backup_file_name)
    print(f"Backup created: {backup_file_name}")

    # Load phone numbers into deque
    phone_numbers = load_phone_numbers(input_file)
    total_numbers = len(phone_numbers)
    print(f"Loaded {total_numbers} phone numbers.")

    # Prepare DataFrame for processed data
    columns = ["Phone", "National DNC", "State DNC", "Blacklisted", "State"]
    df = pd.DataFrame(columns=columns)

def process_data():
    """Process phone numbers."""
    global phone_numbers, df, successful_tcpa_api, failed_tcpa_api

    for idx in range(1, total_numbers + 1):
        if phone_numbers:
            phone = phone_numbers.popleft()

            # Print processing info and counters
            print("==" * 30)
            print(f"Processing {idx}/{total_numbers}: {phone}")
            print(f"  [Stats] Successful TCPA API: {successful_tcpa_api}, Failed TCPA API: {failed_tcpa_api}")

            # Fetch TCPA data
            tcpa_data = fetch_tcpa_data(phone, ".".join(str(random.randint(0, 255)) for _ in range(4)))

            # Combine data and add to DataFrame
            print("==" * 15)
            print(tcpa_data)
            print("==" * 15)
            df = pd.concat([df, pd.DataFrame([tcpa_data])], ignore_index=True)

            # Respectful delay
            time.sleep(1)

def cleanup():
    """Write processed data and save remaining unprocessed numbers."""
    global df, phone_numbers

    # Drop duplicates and write to CSV
    df = df.drop_duplicates(keep="first")
    df.to_csv(output_file, index=False, encoding="utf-8")
    print(f"Processed data written to {output_file}")

    # Save remaining unprocessed data
    save_remaining_data("data.txt", phone_numbers)
    print(f"Remaining data written back to 'data.txt'")

# --------------------------------- Driver of Program -------------------------------------
def driver():
    init()
    process_data()

# --------------------------------- Function Call and Handling -------------------------------------
try:
    exception_occurred = False
    driver()
except Exception as e:
    exception_occurred = True
    print("===" * 30)
    print("ENDING.... EXCEPTION...")
    print("===" * 30)
    print(f"Exception occurred: {e}")
finally:
    cleanup()
    print("Quitting Now")
    if exception_occurred:
        sys.exit(1)  # Exit with a non-zero exit code to indicate error
    else:
        sys.exit(0)
