import os
import sys
import time
import random
from datetime import datetime, timedelta
from bhavcopy import download_nse_data

# --- Configuration ---
START_DATE  = datetime(2025, 3, 22)   # 1 year ago
END_DATE    = datetime(2026, 3, 22)   # today
OUTPUT_PATH = "./bhavcopy_data"       # directory to save downloaded CSVs
# ---------------------

def already_downloaded(date_str, output_path):
    """Check if any file matching this date already exists in output_path."""
    for fname in os.listdir(output_path):
        if date_str.upper() in fname.upper():
            return True
    return False

def main():
    abs_output_path = os.path.abspath(OUTPUT_PATH)
    os.makedirs(abs_output_path, exist_ok=True)

    original_dir = os.getcwd()
    os.chdir(abs_output_path)

    current = START_DATE
    total_days = 0
    downloaded = 0
    skipped = 0
    failed = 0
    holidays = 0

    print(f"Downloading bhavcopy from {START_DATE.strftime('%d-%b-%Y')} to {END_DATE.strftime('%d-%b-%Y')}")
    print(f"Output directory: {abs_output_path}\n")

    while current <= END_DATE:
        # Skip weekends
        if current.weekday() >= 5:
            current += timedelta(days=1)
            continue

        date_str = current.strftime("%d%b%Y").upper()
        total_days += 1

        # Skip if already downloaded
        if already_downloaded(date_str, abs_output_path):
            print(f"[SKIP]     {date_str} — already exists")
            skipped += 1
            current += timedelta(days=1)
            continue

        try:
            result = download_nse_data(date_str)
            if result:
                zip_filename = f"fo{date_str}bhav.csv.zip"
                if os.path.exists(zip_filename):
                    os.remove(zip_filename)
                print(f"[OK]       {date_str} → {result}")
                downloaded += 1
            else:
                print(f"[HOLIDAY]  {date_str} — no data (holiday or non-trading day)")
                holidays += 1
        except Exception as e:
            print(f"[ERROR]    {date_str} — {e}")
            failed += 1

        sleep_sec = random.uniform(2, 7)
        time.sleep(sleep_sec)

        current += timedelta(days=1)

    os.chdir(original_dir)

    print(f"\n--- Summary ---")
    print(f"Weekdays checked : {total_days}")
    print(f"Downloaded       : {downloaded}")
    print(f"Already existed  : {skipped}")
    print(f"Holidays/no data : {holidays}")
    print(f"Errors           : {failed}")

if __name__ == "__main__":
    main()
