import pandas as pd
import schedule
import time
import os
from pandas.errors import EmptyDataError
from datetime import datetime, timedelta

directory_of_python_script = os.path.dirname(os.path.abspath(__file__))
all_files = os.listdir(directory_of_python_script)    
csv_files = list(filter(lambda f: f.endswith('.csv'), all_files))
csv_files_name = [os.path.splitext(each)[0] for each in csv_files]
# print(csv_files)

def data_delete():
    for filename in csv_files_name:
        try:
            df = pd.read_csv(os.path.join(directory_of_python_script, f"{filename}.csv"), index_col=None, header=0)
            datetime_unique = pd.to_datetime(df.iloc[:, 1]).dt.date
            thirty_days_next = str(datetime_unique + timedelta(days=30))
            result = df.drop(df[df.iloc[:, 1] < thirty_days_next].index, inplace=True)
            print(result)
        except EmptyDataError:
            print(f"No columns to parse from file {filename}")    

schedule.every().monday.at("09:00").do(data_delete)
schedule.every().tuesday.at("09:00").do(data_delete)
schedule.every().wednesday.at("09:00").do(data_delete)
schedule.every().thursday.at("09:00").do(data_delete)
schedule.every().friday.at("09:00").do(data_delete)

while True:
    schedule.run_pending()
    time.sleep(1)