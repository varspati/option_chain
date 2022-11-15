import sys
import schedule
import time 
from urllib import response
import pandas as pd
from datetime import datetime as dt, timedelta, date
import datetime
import pyodbc
from pyparsing import col 
import sqlalchemy
from sqlalchemy import create_engine
from multiprocessing import Pool, Process
import requests
from multiprocessing import active_children
from concurrent.futures import ThreadPoolExecutor
from multiprocessing.pool import ThreadPool 
import numpy as np
from apscheduler.schedulers.blocking import BlockingScheduler
import concurrent.futures
import os
from schedule import every, repeat, run_pending



symbols_script = ['NIFTY', 'BANKNIFTY', 'AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT', 'ADANIPORTS', 'ALKEM', 'AMARAJABAT', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'ATUL', 'AUBANK', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COFORGE', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB', 'CUMMINSIND', 'DABUR', 'DALBHARAT', 'DEEPAKNTR', 'DELTACORP', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'FSL', 'GAIL', 'GLENMARK', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GSPL', 'GUJGASLTD', 'HAL', 'HAVELLS', 'HCLTECH', 'HDFC', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HONAUT', 'IBULHSGFIN', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI',
                  'IDFC', 'IEX', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIAMART', 'INDIGO', 'INDUSINDBK', 'INDUSTOWER', 'INFY', 'INTELLECT', 'IOC', 'IPCALAB', 'IRCTC', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JSWSTEEL', 'JUBLFOOD', 'KOTAKBANK', 'L&TFH', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTI', 'LTTS', 'LUPIN', 'M&M', 'M&MFIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCDOWELL-N', 'MCX', 'METROPOLIS', 'MFSL', 'MGL', 'MINDTREE', 'MOTHERSON', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NAM-INDIA', 'NATIONALUM', 'NAUKRI', 'NAVINFLUOR', 'NESTLEIND', 'NMDC', 'NTPC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PETRONET', 'PFC', 'PIDILITIND', 'PIIND', 'POLYCAB', 'POWERGRID', 'PVR', 'RAIN', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBICARD', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SRF', 'SRTRANSFIN', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TITAN', 'TORNTPHARM', 'TORNTPOWER', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VEDL', 'VOLTAS', 'WHIRLPOOL', 'WIPRO', 'ZEEL', 'ZYDUSLIFE']
# print(len(symbols_script))

urls = []

url1 = 'https://www.nseindia.com/api/option-chain-indices?symbol'
url2 = 'https://www.nseindia.com/api/option-chain-equities?symbol'

for item in symbols_script:
    if item == 'NIFTY':
        url = "{}={}".format(url1,item)
        urls.append(url)
    elif item == 'BANKNIFTY':
        url = "{}={}".format(url1,item)
        urls.append(url)
    else:
        url = "{}={}".format(url2,item)
        urls.append(url)

headers = {
    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/104.0.0.0 Safari/537.36",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "en-US,en;q=0.9"
}

def get_last_digits(num, last_digits_count=5):
    x = abs(num) % (10**last_digits_count)
    if x % 5 <= 5:
        return int(x/25) if x % 25 == 0 else (x - x % 25) + 25
    elif 5 < x % 5 < 10:
        return int(x / 50) if x % 50 == 0 else (x - x % 50) + 50
    elif 10 < x % 5 < 15:
        return int(x / 50) if x % 50 == 0 else (x - x % 50) + 50
    elif 15 < x % 5 < 19:
        return int(x / 100) if x % 100 == 0 else (x - x % 100) + 100
    else:
        pass

symbol_list = []

def data_fetch(url):
    with requests.session() as s:
        s.get("https://www.nseindia.com", headers=headers)
        data_fetched = s.get(str(url), headers=headers).json()
        if len(data_fetched) != 0:
            rawop = pd.DataFrame(
                data_fetched['filtered']['data']).fillna(0)
            data = []
            for i in range(0, len(rawop)):
                uy = expdate = callpchng = callchng = calliv = calloi = callcoi = putpchng = putchng = putiv = putoi = putcoi = 0
                stp = rawop['strikePrice'][i]
                expdate = rawop['expiryDate'][i]

                if (rawop['CE'][i] == 0):
                    calloi = callcoi = 0
                else:
                    callpchng = rawop['CE'][i]['pchangeinOpenInterest']
                    callchng = rawop['CE'][i]['change']
                    calliv = rawop['CE'][i]['impliedVolatility']
                    calloi = rawop['CE'][i]['openInterest']
                    callcoi = rawop['CE'][i]['changeinOpenInterest']
                    calluy = rawop['CE'][i]['underlyingValue']
                if (rawop['PE'][i] == 0):
                    putoi = putcoi = 0
                else:
                    putpchng = rawop['PE'][i]['pchangeinOpenInterest']
                    putchng = rawop['PE'][i]['change']
                    putiv = rawop['PE'][i]['impliedVolatility']
                    putoi = rawop['PE'][i]['openInterest']
                    putcoi = rawop['PE'][i]['changeinOpenInterest']
                    putuy = rawop['PE'][i]['underlyingValue']
                opdata = {
                    'CALL_UNDERLYING': calluy, 'CALL_P_CHNG': callpchng, 'CALL_CHNG': callchng, 'CALL_IV': calliv, 'CALL_OI': calloi, 'CALL_CHNG_OI': callcoi, 'STRIKE_PRICE': stp, 'EXPIRY_DATE': expdate, 'PUT_UNDERLYING': putuy, 'PUT_P_CHNG': putpchng, 'PUT_CHNG': putchng, 'PUT_IV': putiv, 'PUT_OI': putoi, 'PUT_CHNG_OI': putcoi}
                data.append(opdata)
            optionchain = pd.DataFrame(data)
            optionchain.drop(['PUT_UNDERLYING'], axis=1, inplace=True)
            t = [datetime.datetime.now().replace(microsecond=0) for i in optionchain.index]
            s = pd.Series(t, name='TimeStamp')
            optionchain.insert(0, 'TimeStamp', s)
            symbols_table = url.split('=')[1]
            # print(symbols_table)
            optionchain.insert(1, 'Symbol', symbols_table)
            optionchain.rename(columns={'CALL_UNDERLYING': 'UNDERLYING'}, inplace=True)
            optionchain['UNDERLYING'] = optionchain['UNDERLYING'].fillna(0).astype(int)
            optionchain['UNDERLYING'] = optionchain.apply(lambda x: get_last_digits(x['UNDERLYING']), axis=1)
            val = optionchain['UNDERLYING'].values[0]
            idx = (optionchain['STRIKE_PRICE']-val).abs().idxmin()
            PCR_calc = optionchain.iloc[idx - 12: idx + 13]
            PCR_calc['PCR'] = PCR_calc['PUT_CHNG_OI'] - PCR_calc['CALL_CHNG_OI']
            PCR_calc['PCR_avrg'] = PCR_calc['PCR'].mean()
            PCR_calc['resistance'] = PCR_calc['CALL_OI'].nlargest(3)
            PCR_calc['support'] = PCR_calc['PUT_OI'].nlargest(3)
            PCR_calc.replace({np.inf: np.nan, -np.inf: np.nan}, inplace=True)
            PCR_calc = PCR_calc.fillna(0)
            # print(PCR_calc)
            # print(PCR_calc.columns.tolist())
            directory_of_python_script = os.path.dirname(os.path.abspath(__file__))
            # print(directory_of_python_script)
            if PCR_calc.empty:
                PCR_calc = PCR_calc.replace('NULL', np.nan) 
                PCR_calc = PCR_calc[['TimeStamp', 'Symbol', 'UNDERLYING', 'CALL_P_CHNG', 'CALL_CHNG', 'CALL_IV', 'CALL_OI', 'CALL_CHNG_OI', 'STRIKE_PRICE', 'EXPIRY_DATE', 'PUT_P_CHNG', 'PUT_CHNG', 'PUT_IV', 'PUT_OI', 'PUT_CHNG_OI', 'PCR', 'PCR_avrg', 'resistance', 'support']]
                PCR_calc.to_csv(os.path.join(directory_of_python_script, f"{symbols_table}.csv"), mode='a', header=False)
                # PCR_calc.to_csv(f'C:\\Users\\varspati\\OneDrive - Cisco\\Desktop\\Trade Project\Option_Chain_data\\{symbols_table}.csv', mode='a')
            else:
                if '-' in symbols_table or '&' in symbols_table:
                    PCR_calc = PCR_calc[['TimeStamp', 'Symbol', 'UNDERLYING', 'CALL_P_CHNG', 'CALL_CHNG', 'CALL_IV', 'CALL_OI', 'CALL_CHNG_OI', 'STRIKE_PRICE', 'EXPIRY_DATE', 'PUT_P_CHNG', 'PUT_CHNG', 'PUT_IV', 'PUT_OI', 'PUT_CHNG_OI', 'PCR', 'PCR_avrg', 'resistance', 'support']]
                    PCR_calc.to_csv(os.path.join(directory_of_python_script, f"{symbols_table}.csv"), mode='a', header=False)
                    # PCR_calc.to_csv(f'C:\\Users\\varspati\\OneDrive - Cisco\\Desktop\\Trade Project\\Option_Chain_data\\{symbols_table}.csv', mode='a')
                else:
                    PCR_calc = PCR_calc[['TimeStamp', 'Symbol', 'UNDERLYING', 'CALL_P_CHNG', 'CALL_CHNG', 'CALL_IV', 'CALL_OI', 'CALL_CHNG_OI', 'STRIKE_PRICE', 'EXPIRY_DATE', 'PUT_P_CHNG', 'PUT_CHNG', 'PUT_IV', 'PUT_OI', 'PUT_CHNG_OI', 'PCR', 'PCR_avrg', 'resistance', 'support']]
                    PCR_calc.to_csv(os.path.join(directory_of_python_script, f"{symbols_table}.csv"), mode='a', header=False)
                    # PCR_calc.to_csv(f'C:\\Users\\varspati\\OneDrive - Cisco\\Desktop\\Trade Project\Option_Chain_data\\{symbols_table}.csv', mode='a')
        else:
            pass
    return ''   

# data_fetch(url=url)

start = dt.strptime("09:15:00", "%H:%M:%S")
end = dt.strptime("15:35:00", "%H:%M:%S")
# min_gap
min_gap = 3

# compute datetime interval
arr = [(start + timedelta(hours=min_gap*i/60)).strftime("%H:%M:%S")
       for i in range(int((end-start).total_seconds() / 60.0 / min_gap))]
# print(arr) 


def execute_script():
    with concurrent.futures.ThreadPoolExecutor() as executor:
        executor.map(data_fetch, urls)
        
for i in arr:
    schedule.every().monday.at(i).do(execute_script)
    schedule.every().tuesday.at(i).do(execute_script)
    schedule.every().wednesday.at(i).do(execute_script)
    schedule.every().thursday.at(i).do(execute_script)
    schedule.every().friday.at(i).do(execute_script)

while True:
    schedule.run_pending()
    time.sleep(30)