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
import os
from datetime import date, datetime, timedelta, time as t
import csv, re, time, math

time_1 = t(9,15)
time_2 = t(15,30)

expiry='27-Nov-2024'
today = datetime.now().strftime('%Y-%m-%d')
yesterday = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
order = 0
stoploss=15
exit=''

last_row=None
one_tick=None
previous_tick=None
breeze.ws_connect()

def on_ticks(ticks):
    global one_tick
    print("-------------------------------------------------------------")
    one_tick=ticks

    print(ticks)


breeze.on_ticks = on_ticks



def initiate_ws(atm,right=''):
    if right=='call':    
        # print("hello")
        leg=breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="CNXBAN",
                                product_type="options",
                                # expiry_date=f'{expiry}T06:00:00.000Z',
                                expiry_date=expiry,
                                right="call",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        print(leg)
    elif right=='put':
        leg2=breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="CNXBAN",
                                product_type="options",
                                expiry_date=expiry,
                                right="put",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        print(leg2)
        

def deactivate_ws(atm,right=''):
    if right=='call':    
        breeze.unsubscribe_feeds(exchange_code="NFO",
                            stock_code="CNXBAN",
                            product_type="options",
                            expiry_date=expiry,
                            right="call",
                            strike_price=str(atm),
                            get_exchange_quotes=True,
                            get_market_depth=False)
    elif right=='put':
        breeze.unsubscribe_feeds(exchange_code="NFO",
                            stock_code="CNXBAN",
                            product_type="options",
                            expiry_date=expiry,
                            right="put",
                            strike_price=str(atm),
                            get_exchange_quotes=True,
                            get_market_depth=False)

# def save_to_csv(path,mode,df):

def adjust_trailing_sl(premium, sl, order):
    """Adjust the trailing stop-loss based on the current price and factor."""
    if order in [1, -1]:
        new_sl = premium - 15 
        return max(new_sl, sl)


def one_minute_data():

    data =breeze.get_historical_data_v2(interval="1minute",
                                        from_date=f'{yesterday}T07:00:00.000Z',
                                        to_date=f'{today}T17:00:00.000Z',
                                        stock_code='CNXBAN',
                                        product_type='CASH',
                                        exchange_code='NSE')
    if 'Success' in data:
        olhc = data['Success']
        olhc=pd.DataFrame(olhc)
        olhc['datetime']=pd.to_datetime(olhc['datetime'])
        olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        olhc['12_EMA']=olhc['close'].ewm(span=12,adjust=False).mean()
        olhc['26_EMA']=olhc['close'].ewm(span=26,adjust=False).mean()
        olhc['MACD_Line']=olhc['12_EMA']-olhc['26_EMA']
        olhc['Signal_Line']=olhc['MACD_Line'].ewm(span=9,adjust=False).mean()
        olhc['Histogram']=olhc['MACD_Line']-olhc['Signal_Line']
        olhc['MACD']=olhc['MACD_Line']

        olhc['close'] = pd.to_numeric(olhc['close'])
        olhc.ta.rsi(close='close', length=14, append=True)

        return olhc
    
while True:
    now = datetime.now()
    
    if (time_1 < t(datetime.now().time().hour, datetime.now().time().minute) < time_2) and (now.second == 0):
        olhc=one_minute_data()

        last_row=olhc.iloc[-1]
        second_last=olhc.iloc[-2]
        third_last=olhc.iloc[-3]

        if order==0:
            if last_row['RSI_14']>=70 and second_last['RSI_14']>=70 and last_row['MACD']>second_last['MACD']>third_last['MACD']:
                entry_time=datetime.now().strftime('%H:%M:%S')
                order=1
                atm = round(last_row['close'] / 100) * 100
                print(atm)
                initiate_ws(atm,'call')
                # try:
                time.sleep(2)
                # buy_price=float(one_tick['last'])
                # except:
                leg = breeze.get_option_chain_quotes(stock_code="CNXBAN",
                                                    exchange_code="NFO",
                                                    product_type="options",
                                                    expiry_date=f'{expiry}T06:00:00.000Z',
                                                    right="call",
                                                    strike_price=atm)
                leg = leg['Success']
                leg = pd.DataFrame(leg)
                buy_price = float(leg['ltp'][0])
                
                sl=buy_price-stoploss
                # deactivate_ws(atm,'call')
                print(now, 'buy', atm, 'call at:', buy_price)

            elif last_row['RSI_14']<30 and second_last['RSI_14']<30 and last_row['MACD']<second_last['MACD']<third_last['MACD']:
                entry_time=datetime.now().strftime('%H:%M:%S')
                order=-1
                atm = round(last_row['close'] / 100) * 100
                initiate_ws(atm,'put')
                time.sleep(5)
                # buy_price=float(one_tick['last'])
                leg = breeze.get_option_chain_quotes(stock_code="CNXBAN",
                                                    exchange_code="NFO",
                                                    product_type="options",
                                                    expiry_date=f'{expiry}T06:00:00.000Z',
                                                    right="put",
                                                    strike_price=atm)
                leg = leg['Success']
                leg = pd.DataFrame(leg)
                buy_price = float(leg['ltp'][0])
                sl=buy_price-stoploss
                # deactivate_ws(atm,'put')
                print(now, 'buy', atm, 'put at:', buy_price)
            else:
                print(last_row['datetime'], 'no trade condition: rsi is ', last_row['RSI_14'], 'macd is ', last_row['MACD'])
    if order==1 and last_row is not None:
        premium=one_tick['last']
        
        if premium >= buy_price-sl:
                # old_sl=sl
                sl = adjust_trailing_sl(premium, sl, order)
                print(f"Stop Loss trailed. Premium: {premium}, New SL: {sl}")
                # logging.info(f"Stop Loss trailed. Premium: {one_tick['last']}, New SL: {sl}")
                
        if last_row['MACD'] < second_last['MACD'] and second_last['MACD'] < third_last['MACD'] and premium<=sl :
            order = 0
            # initiate_ws(atm,'call')
            exit_time = datetime.now().strftime('%H:%M:%S')
            if one_tick is not None:
                sell_price=float(premium)
                pnl = round(sell_price - buy_price, 2)
                print(now, 'exit', atm, 'call pnl is:', pnl)
                deactivate_ws(atm,right='call')
                write_data=pd.DataFrame([[today, entry_time, atm, 'put', buy_price, exit_time, sell_price, pnl]],columns=['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL'])

                csv_file = "Paper_trade_BN_macd.csv"
                if os.path.exists(csv_file):
                        write_data.to_csv(csv_file,header=False,mode='a',index=False)

                else:
                    write_data.to_csv(csv_file,header=True,index=False)
    if order==-1 and last_row is not None:
        premium=one_tick['last']
        if premium >= buy_price-sl:
                sl = adjust_trailing_sl(premium, sl, order)
                print(f"Stop Loss trailed. Premium: {premium}, New SL: {sl}")
        if last_row['MACD'] > second_last['MACD'] and second_last['MACD'] > third_last['MACD'] and premium<=sl:
            order = 0
            # initiate_ws(atm,right='put')
            exit_time = datetime.now().strftime('%H:%M:%S')
            if one_tick is not None:
                sell_price=float(premium)
                pnl = round(sell_price - buy_price, 2)
                print(now, 'exit', atm, 'put pnl is:', pnl)
                deactivate_ws(atm,right='put')
                write_data=pd.DataFrame([[today, entry_time, atm, 'call', buy_price, exit_time, sell_price, pnl]],columns=['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium','Exit Time', 'Exit premium', 'PnL'])

                csv_file = "Paper_trade_BN_macd.csv"
                if os.path.exists(csv_file):
                        write_data.to_csv(csv_file,header=False,mode='a',index=False)
                else:
                    write_data.to_csv(csv_file,header=True,index=False)
        else:
                print(last_row['datetime'], 'no exit conditon: rsi is ', last_row['RSI_14'], 'macd is ', last_row['MACD'])
    
    if (time_1 < t(datetime.now().time().hour, datetime.now().time().minute) > time_2):
        breeze.ws_disconnect()
        quit()
        # print("----------------------------------------")      
    time.sleep(1)
