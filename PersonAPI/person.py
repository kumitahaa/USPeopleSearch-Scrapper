import pandas as pd
import time, requests, random, sys
from collections import deque
from datetime import datetime
import shutil

# Constants
PERSON_API_URL = "https://person.api.uspeoplesearch.net/person/v3"
PI_DEFAULT = ".".join(str(random.randint(0, 255)) for _ in range(4))
TIMEOUT = 10
BACKUP_FILE = "data_old.txt"

# Globals for counters
successful_person_api = 0
failed_person_api = 0

def fetch_person_data(phone, pi=PI_DEFAULT, retries=2):
    """Fetch Person data for a given phone number with retries."""
    global successful_person_api, failed_person_api
    params = {'x': phone, 'pi': pi}
    default_person = {
        "Phone": phone,
        "Name": "ZZZ", "DoB": "ZZZ", "Age": "ZZZ",
        "Address": "ZZZ", "City": "ZZZ", "State": "ZZZ", "ZIP": "ZZZ"
    }

    for attempt in range(1, retries + 1):
        try:
            response = requests.get(PERSON_API_URL, params=params, timeout=TIMEOUT)
            response.raise_for_status()
            print(f"  [PERSON_API Response]: {response.text}")  # Print the raw response
            data = response.json()
            persons = data.get("person", [])
            if not persons:
                raise ValueError("Empty person data returned.")

            # Extract first person's details
            first_person = persons[0]
            addresses = first_person.get("addresses", [])
            first_address = addresses[0] if addresses else {}

            successful_person_api += 1
            return {
                "Phone": phone,
                "Name": first_person.get("name", "ZZZ"),
                "DoB": first_person.get("dob", "ZZZ"),
                "Age": first_person.get("age", "ZZZ"),
                "Address": first_address.get("home", "ZZZ"),
                "City": first_address.get("city", "ZZZ"),
                "State": first_address.get("state", "ZZZ"),
                "ZIP": first_address.get("zip", "ZZZ")
            }
        except (requests.RequestException, ValueError) as e:
            print(f"  [Person Error] Attempt {attempt} failed for {phone}: {e}")
            if attempt == retries:
                failed_person_api += 1
                return default_person
        time.sleep(1)  # Short delay before retry

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
    global phone_numbers, total_numbers, df

    input_file = "data.txt"
    backup_file_name = BACKUP_FILE
    

    # Backup original file
    backup_file(input_file, backup_file_name)
    print(f"Backup created: {backup_file_name}")

    # Load phone numbers into deque
    phone_numbers = load_phone_numbers(input_file)
    total_numbers = len(phone_numbers)
    print(f"Loaded {total_numbers} phone numbers.")

    # Prepare DataFrame for processed data
    columns = ["Phone", "Name", "DoB", "Age", "Address", "City", "State", "ZIP"]
    df = pd.DataFrame(columns=columns)

def process_data():
    """Process phone numbers."""
    global phone_numbers, df, successful_person_api, failed_person_api

    for idx in range(1, total_numbers + 1):
        if phone_numbers:
            phone = phone_numbers.popleft()

            # Print processing info and counters
            print("==" * 30)
            print(f"Processing {idx}/{total_numbers}: {phone}")
            print(f"  [Stats] Successful Person API: {successful_person_api}, Failed Person API: {failed_person_api}")

            # Fetch Person data with retries
            person_data = fetch_person_data(phone, ".".join(str(random.randint(0, 255)) for _ in range(4)))

            # Combine data and add to DataFrame
            print("==" * 15)
            print(person_data)
            print("==" * 15)
            df = pd.concat([df, pd.DataFrame([person_data])], ignore_index=True)

            # Respectful delay
            time.sleep(1)

def cleanup():
    """Write processed data and save remaining unprocessed numbers."""
    global df, phone_numbers, output_file

    # Drop duplicates and write to CSV
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"{timestamp}_person.csv"
    
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
