import requests
from datetime import date
from datetime import datetime
import time
import zipfile
import os
from datetime import datetime, timedelta

def get_weekdays(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        if current_date.weekday() < 5:  # Monday to Friday have weekday values 0 to 4
            yield current_date
        current_date += timedelta(days=1)


Month = {
    1: 'JAN',
    2: 'FEB',
    3: 'MAR',
    4: 'APR',
    5: 'MAY',
    6: 'JUN',
    7: 'JUL',
    8: 'AUG',
    9: 'SEP',
    10: 'OCT',
    11: 'NOV',
    12: 'DEC'
}


def download( date1):
    global date
    if( date1 == ""):
       today = date.today()
       date_str = today.strftime("%Y%m%d").upper()
       year = today.year
       month = today.month
       date = today.day

    else:
        date_str = date1
        date = datetime.strptime(date_str, "%Y%m%d").date()
        year = date.year
        month = date.month
        date = date.day
    if( date < 10):
        date = '0' + str( date)
    print( date, month, year)
#    return
    url = f"https://nsearchives.nseindia.com/content/historical/DERIVATIVES/{year}/{Month[month]}/fo{date}{Month[month]}{year}bhav.csv.zip"
    #url = "javascript:;"
    print( url)
    # Set headers to mimic a browser request
    headers = {
        'User-Agent': 'Mozilla/5.0'
    }

    # Send a request to the NSE website
    response = requests.get(url, headers=headers)
#    print( response.content)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Create a directory with today's date
        directory = 'OI_DATA'
        os.makedirs(directory, exist_ok=True)

        # Save the content of the response to a file
        #file_path = os.path.join(directory, f"cm{date_str}bhav.csv.zip")
        file_path = os.path.join(directory, f"fo{date_str}bhav.csv")
        print(file_path)
        with open(file_path, "wb") as file:
            file.write(response.content)
            file.close()
        print(f"File {file_path} downloaded successfully.")

        zip_file_path = file_path
        extracted_dir = directory

        with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
            zip_ref.extractall(extracted_dir)

        try:
            os.remove(file_path)
        except OSError as e:
            print(f'Error: {e.filename} - {e.strerror}')

#download("20231013")

#%%s


start_date = datetime(2023, 12, 11)  # Example start date (year, month, day)
end_date = datetime(2023, 12, 12)   # Example end date (year, month, day)

#for date in get_weekdays(start_date, end_date):
#current_date = datetime(2023, 10, 19)
current_date = end_date
step  =timedelta(days = -1)
while current_date >= start_date:
    date = current_date
    print(date.strftime("%Y%m%d"))
    date1 = date.strftime("%Y%m%d")
    time.sleep(5)
    download(date1)
    current_date += step
