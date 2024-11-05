# from breeze_connect import BreezeConnect
# breeze = BreezeConnect(api_key="d96783Qp368558*55FI36Z24W0ET39Lf")
# import urllib
# breeze.generate_session(api_secret="58836g597W4l8977h7~%eX9967^807x3",
#                         session_token="48857984")
from breeze1 import *

import numpy as np
import pandas as pd
import pandas_ta as ta
import time
from datetime import date, datetime, timedelta, time as t
import csv, re, time, math

time_1 = t(9,15)
time_2 = t(15,30)

expiry = '2024-11-06'
order = 0
#qty = 25

while True:
    now = datetime.now()
    if (time_1 < t(datetime.now().time().hour, datetime.now().time().minute) < time_2) and (now.second == 0):
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        
        data = breeze.get_historical_data_v2(interval="1minute",
                                                from_date= f"{yesterday}T07:00:00.000Z",
                                                to_date= f"{today}T17:00:00.000Z",
                                                stock_code="CNXBAN",
                                                exchange_code="NSE",
                                                product_type="cash")
                
        
        olhc = data['Success']
        olhc = pd.DataFrame(olhc)
        olhc['datetime'] = pd.to_datetime(olhc['datetime'])
        olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                       (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
        olhc['12_EMA'] = olhc['close'].ewm(span=12, adjust=False).mean()
        olhc['26_EMA'] = olhc['close'].ewm(span=26, adjust=False).mean()
        olhc['MACD_Line'] = olhc['12_EMA'] - olhc['26_EMA']
        olhc['Signal_Line'] = olhc['MACD_Line'].ewm(span=9, adjust=False).mean()
        olhc['MACD_Histogram'] = olhc['MACD_Line'] - olhc['Signal_Line']
        olhc['MACD'] = olhc['MACD_Line']
        
        olhc['close'] = pd.to_numeric(olhc['close'])
        olhc.ta.rsi(close='close', length=14, append=True)
        
        last_row = olhc.iloc[-1]
        second_last = olhc.iloc[-2]
        third_last = olhc.iloc[-3]
        
        
        if order == 0 :
            if last_row['RSI_14']>70 and second_last['RSI_14']>70 and last_row['MACD']>second_last['MACD'] and second_last['MACD']>third_last['MACD'] :
                entry_time = datetime.now().strftime('%H:%M:%S')
                order = 1
                atm = round(last_row['close'] / 100) * 100
                j=1
                for i in range(j):
                    leg = breeze.get_option_chain_quotes(stock_code="CNXBAN",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="call",
                                                            strike_price=atm)
                    if leg['Status']==200:
                        leg = leg['Success']
                        leg = pd.DataFrame(leg)
                        buy_price = float(leg['ltp'][0])
                        
                        print(now, 'buy', atm, 'call at:', buy_price)
                    else:
                        j+=1
                
            elif last_row['RSI_14']<30 and second_last['RSI_14']<30 and last_row['MACD']<second_last['MACD'] and second_last['MACD']<third_last['MACD'] :
                entry_time = datetime.now().strftime('%H:%M:%S')
                order = -1
                atm = round(last_row['close'] / 100) * 100
                j=1
                for i in range(j):
                    leg = breeze.get_option_chain_quotes(stock_code="CNXBAN",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="put",
                                                            strike_price=atm)
                    
                    if leg['Status']==200:
                        leg = leg['Success']
                        leg = pd.DataFrame(leg)
                        buy_pe_price = float(leg['ltp'][0])
                        
                        print(now, 'buy', atm, 'put at:', buy_pe_price)
                    else:
                        j+=1
            else:
                print(last_row['datetime'], 'no trade condition: rsi is ', last_row['RSI_14'], 'macd is ', last_row['MACD'])
                    
                
        if order == 1 :
            if last_row['MACD'] < second_last['MACD'] and second_last['MACD'] < third_last['MACD'] :
                order = 0
                exit_time = datetime.now().strftime('%H:%M:%S')
                
                j=1
                for i in range(j):
                    leg = breeze.get_option_chain_quotes(stock_code="CNXBAN",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="call",
                                                            strike_price=atm)
                    if leg['Status']==200:
                        leg = leg['Success']
                        leg = pd.DataFrame(leg)
                        sell_price = float(leg['ltp'][0])
                        
                        pnl = round(sell_price - buy_price, 2)
                        print(now, 'exit', atm, 'call pnl is:', pnl)
                    else:
                        j+=1
                
                csv_file = "Paper_trade_BN_macd.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, entry_time, atm, 'call', buy_price, exit_time, sell_price, pnl])
                
                
            else:
                print(last_row['datetime'], 'no exit condition: rsi is ', last_row['RSI_14'], 'macd is ', last_row['MACD'])
                
        if order == -1 :
            if last_row['MACD'] > second_last['MACD'] and second_last['MACD'] > third_last['MACD'] :
                order = 0
                exit_time = datetime.now().strftime('%H:%M:%S')
                j=1
                for i in range(j):
                    leg = breeze.get_option_chain_quotes(stock_code="CNXBAN",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="put",
                                                            strike_price=atm)
                    if leg['Status']==200:
                        leg = leg['Success']
                        leg = pd.DataFrame(leg)
                        sell_pe_price = float(leg['ltp'][0])
                        
                        pnl = round(sell_pe_price - buy_pe_price, 2)
                        print(now, 'exit', atm, 'put pnl is:', pnl)
                    else:
                        j+=1
                
                csv_file = "Paper_trade_BN_macd.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, entry_time, atm, 'put', buy_pe_price, exit_time, sell_pe_price, pnl])
                    
            else:
                print(last_row['datetime'], 'no exit conditon: rsi is ', last_row['RSI_14'], 'macd is ', last_row['MACD'])
                
        time.sleep(5)
                
                
