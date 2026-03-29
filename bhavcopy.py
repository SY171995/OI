import requests
import time
import zipfile
import os

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br"
}

def download_nse_data(date_str):
    # date_str format: "12DEC2025" for legacy, "20251212" for UDiFF
    # You might need to adjust your input parsing to get both formats.
    # Assuming input is just the date object or string "12DEC2025"
    
    # URL 1: Legacy Format
    legacy_url = f"https://nsearchives.nseindia.com/content/fo/fo{date_str}bhav.csv.zip"
    
    # URL 2: New UDiFF Format (Requires YYYYMMDD)
    # Convert "12DEC2025" -> "20251212"
    from datetime import datetime
    dt = datetime.strptime(date_str, "%d%b%Y")
    udiff_date = dt.strftime("%Y%m%d")
    udiff_url = f"https://nsearchives.nseindia.com/content/fo/BhavCopy_NSE_FO_0_0_0_{udiff_date}_F_0000.csv.zip"

    session = requests.Session()
    session.headers.update(HEADERS)

    # 1. Initialize Session
    try:
        session.get("https://www.nseindia.com", timeout=10)
    except Exception:
        pass # Proceed anyway, sometimes direct works if headers are strong

    # 2. Try Legacy Download
    print(f"Attempting Legacy: {legacy_url}")
    resp = session.get(legacy_url, timeout=15)
    
    if resp.status_code == 200:
        zip_filename = f"fo{date_str}bhav.csv.zip"
        with open(zip_filename, "wb") as f:
            f.write(resp.content)
        print("✅ Downloaded Legacy Format")

        # Unzip the file
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            extracted_files = zip_ref.namelist()
            zip_ref.extractall()
        os.remove(zip_filename)
        print(f"✅ Extracted and removed {zip_filename}")
        return extracted_files[0] if extracted_files else None

    # 3. Try UDiFF Download (Fallback)
    print(f"⚠️ Legacy 404. Attempting UDiFF: {udiff_url}")
    resp = session.get(udiff_url, timeout=15)
    
    if resp.status_code == 200:
        zip_filename = f"fo{date_str}bhav.csv.zip"
        with open(zip_filename, "wb") as f: # Save with old name for compatibility
            f.write(resp.content)
        print("✅ Downloaded UDiFF Format")

        # Unzip the file
        with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
            extracted_files = zip_ref.namelist()
            zip_ref.extractall()
        os.remove(zip_filename)
        print(f"✅ Extracted and removed {zip_filename}")
        return extracted_files[0] if extracted_files else None
    else:
        print(f"❌ Failed both formats (Status {resp.status_code})")
        return None

# Usage
if __name__ == "__main__":
    download_nse_data("12DEC2025")

