import pandas as pd
import time
import pandas as pd
import time
from datetime import datetime, timedelta, time as t

import csv
import logging
import math

import requests
import json

# Replace these with your actual keys
consumer_key = "d372cccae1c5b06bb5ae64"
consumer_secret = "Qxku045#zW"


# URL for token generation
auth_url = "https://ttblaze.iifl.com/apimarketdata/auth/login"

# Request payload
payload = {
    "secretKey": consumer_secret,
    "appKey": consumer_key,
    "source": "WEBAPI"
}

# Send POST request for authentication
response = requests.post(auth_url, json=payload)

# Process response
if response.status_code == 200:
    data = response.json()
    #print(data)
    access_token = data.get("result", {}).get("token")
    print("Access token received:", access_token)
else:
    print("Error:", response.text)


import requests
import json

# Replace these with your actual keys
interactive_key = "def30df21fe5fd3ec4e627"
interactive_secret = "Qrfk434@vM"


# URL for token generation
url = "https://ttblaze.iifl.com/interactive/user/session"

# Request payload
payload = {
    "secretKey": interactive_secret,
    "appKey": interactive_key,
    "source": "WEBAPI"
}

# Send POST request for authentication
response = requests.post(url, json=payload)

# Process response
if response.status_code == 200:
    data = response.json()
    #print(data)
    interactive_access_token = data.get("result", {}).get("token")
    print("Access token received:", interactive_access_token)
else:
    print("Error:", response.text)

from blaze_api import get_nifty_future_instrument_id, get_nifty_future_ohlc_with_retry, get_ltp_with_retry, get_nifty_option_instrument, get_nifty_option_ohlc_with_retry, get_order_detail, place_options_order 

Call_Buy = None
Put_Buy = None
factor = None
volume_high = None
volume_low = None
vol = None
avg_volume = None
vol_call = None
vol_put = None
avg_volume_call = None
avg_volume_put = None
move_sl_to_cost = False
orb = False
time_1 = t(9, 15)
time_2 = t(15, 30)
target = 30
stoploss = 15
order = 2
quantity="25"
today = datetime.now().strftime("%Y-%m-%d")
#fut_expiry  = "2024-10-31"
expiry_1 = datetime(2024, 11, 28)
option_expiry_date = "07Nov2024"
# Define the parameters for the GET request
exchange_segment = 2  # NSEFO for futures
series = "FUTIDX"
symbol = "NIFTY"
stock_code = "NIFTY"
# Example usage
expiry_date = "28Nov2024" 
start_time = datetime.now().strftime("%b %d %Y 091500")  
end_time = datetime.now().strftime("%b %d %Y 153000") 

# Configure logging
logging.basicConfig(level=logging.DEBUG, filename='IIFL_ORB_pt.log', filemode='w',
                    format='%(asctime)s - %(levelname)s - %(message)s')

log_file = "IIFL_ORB_pt.csv"
headers = ['Date', 'Time', 'Entry Price', 'BUY/SELL', 'Exit Price','Vol Mul', 'Option Volume Mul', 'Exit Time','Strike Price', 'Exit Reason', 'PNL']

# Create file and write headers if file doesn't exist
try:
    with open(log_file, 'x', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(headers)
except FileExistsError:
    pass
   
# Function to log trade information to CSV
def log_trade_to_csv(today, entry_time, entry_price, direction, exit_price, vol, option_vol_avg, exit_time, strike_price, exit_reason, pnl):
    with open(log_file, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow([today, entry_time, entry_price, direction, exit_price,vol, option_vol_avg, exit_time, strike_price, exit_reason, pnl])

def get_volume_factor(volume, avg_volume):
    """Determine the volume factor based on the current volume and average volume."""
    if volume > (avg_volume * 2.5):
     
        return 1
    return None

def adjust_trailing_sl(current_price, sl, factor, order):
    """Adjust the trailing stop-loss based on the current price and factor."""
    if order in [1, -1]:
        new_sl = current_price - 15 
    return max(new_sl, sl)

def round_to_nearest_50(n):
    return round(n / 50) * 50

def update_volume_conditions(factor, last_row):
    """Update volume high and low based on the latest volume spike candle."""
    global volume_high, volume_low
    volume_high = last_row['high']
    volume_low = last_row['low']
    logging.info(f"{datetime.now()} Volume condition met with factor: {factor}, High={volume_high}, Low={volume_low}")

d = 0.012
r = 0.068

    
def d1(up, sp, t, r, v, d):
    if up <= 0 or sp <= 0:
        raise ValueError(f"Invalid prices: up={up}, sp={sp}")
    if t <= 0:
        raise ValueError(f"Invalid time to expiry: t={t}")
    if v <= 0:
        raise ValueError(f"Invalid volatility: v={v}")
    return (math.log(up / sp) + (r - d + 0.5 * v ** 2) * t) / (v * math.sqrt(t))


def nd1(up, sp, t, r, v, d):
    return math.exp(-(d1(up, sp, t, r, v, d) ** 2) / 2) / (math.sqrt(2 * math.pi))
    
def d2(up, sp, t, r, v, d):
    return d1(up, sp, t, r, v, d) - v * math.sqrt(t)

def nd2(up, sp, t, r, v, d):
    return 0.5 * (1 + math.erf(d2(up, sp, t, r, v, d) / math.sqrt(2)))
    
def call_price(up, sp, t, r, v, d):
    return math.exp(-d * t) * up * nd1(up, sp, t, r, v, d) - sp * math.exp(-r * t) * nd1(up, sp, t, r, v, d - v * math.sqrt(t))

def put_price(up, sp, t, r, v, d):
    return sp * math.exp(-r * t) * (1 - nd2(up, sp, t, r, v, d)) - math.exp(-d * t) * up * (1 - nd1(up, sp, t, r, v, d))

def call_iv(up, sp, t, r, mp, d):
    mx = 5
    mn = 0
    while (mx - mn) > 0.0001:
        if call_price(up, sp, t, r, (mx + mn) / 2, d) > mp:
            mx = (mx + mn) / 2
        else:
            mn = (mx + mn) / 2
    return (mx + mn) / 2

def put_iv(up, sp, t, r, mp, d):
    mx = 5
    mn = 0
    while (mx - mn) > 0.0001:
        if put_price(up, sp, t, r, (mx + mn) / 2, d) > mp:
            mx = (mx + mn) / 2
        else:
            mn = (mx + mn) / 2
    return (mx + mn) / 2

    

def option_vega(up, sp, t, r, v, d):
    return 0.01 * up * math.sqrt(t) * nd1(up, sp, t, r, v, d)

def olhc_func(ohlc_data):
    candle_data = ohlc_data['result']['dataReponse'].split(',')
    
    # Prepare list to store each candle's OHLC data
    ohlc_list = []
    
    # Loop through each entry and split the candle components
    for candle in candle_data:
        components = candle.split('|')
        timestamp = int(components[0])  # Assuming timestamp is in epoch format
        open_price = float(components[1])
        high_price = float(components[2])
        low_price = float(components[3])
        close_price = float(components[4])
        volume = int(components[5])
        
        # Append to the list
        ohlc_list.append([timestamp, open_price, high_price, low_price, close_price, volume])

    ohlc_df = pd.DataFrame(ohlc_list, columns=['Timestamp', 'open', 'high', 'low', 'close', 'volume'])
    ohlc_df['Timestamp'] = pd.to_datetime(ohlc_df['Timestamp'], unit='s')    
    return ohlc_df

# Main loop
while True:
    now = datetime.now()

    # ORB Breakout Condition
    if time_1 < t(now.hour, now.minute) < time_2 and order == 0 and now.second == 1:
        print(f"Checking ORB conditions at {now}")
        logging.info(f"Checking ORB conditions at {now}")
        response = get_nifty_future_with_retry(expiry_date)
        if response.status_code == 200:
            orb_hist = response.json()
            olhc = pd.DataFrame(orb_hist['Success'])  # Adjust based on the actual data structure
        else:
            print("Error fetching Nifty Future data:", response.text)
        first_row = olhc.iloc[0]
        last_row = olhc.iloc[-1]
        
        if first_row['high'] < last_row['close']:
            breakout_candle_high = first_row['high']
            breakout_highs = olhc[olhc['high'] > breakout_candle_high]
            if not breakout_highs.empty:
                breakout_high = breakout_highs.iloc[0]['high']
                subsequent_highs = olhc[olhc['high'] > breakout_high]
                if not subsequent_highs.empty:
                    current_time = datetime.now()

                    ltp = get_ltp_with_retry(access_token, "1", "26000")
                    strike_price = round_to_nearest_50(ltp)

                    option_instrument_id = get_nifty_option_instrument(access_token, option_expiry_date, strike_price, "CE")            
                        
                    app_order_id = place_options_order(interactive_access_token, option_instrument_id, quantity, "BUY")
                    premium = get_order_detail(interactive_access_token, app_order_id)   

                    Call_Buy = round(premium, 2)
                    right = 'BUY'
                    entry_time = datetime.now().strftime('%H:%M:%S')
                    tgt = Call_Buy + target
                    sl = Call_Buy - stoploss  # Set initial SL
                    order = 1
                    orb = True
                    entry_time = datetime.strptime(entry_time, '%H:%M:%S')
                    entry_time = current_time.replace(hour=entry_time.hour, minute=entry_time.minute, second=entry_time.second, microsecond=0)
                    print(f"Call Buy at: {Call_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
                    logging.info(f"Call Buy at: {Call_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
            else:
                print("No Bullish Position")
                logging.info("No Bullish Position")
        
        if first_row['low'] > last_row['close']:
            breakout_candle_low = first_row['low']
            breakout_lows = olhc[olhc['low'] < breakout_candle_low]
            if not breakout_lows.empty:
                breakout_low = breakout_lows.iloc[0]['low']
                subsequent_lows = olhc[olhc['low'] < breakout_low]
                if not subsequent_lows.empty:
                    current_time = datetime.now()
                    ltp = get_ltp_with_retry(access_token, "1", "26000")
                    strike_price = round_to_nearest_50(ltp)

                    option_instrument_id = get_nifty_option_instrument(access_token, option_expiry_date, strike_price, "PE")            
                        
                    app_order_id = place_options_order(interactive_access_token, option_instrument_id, quantity, "BUY")
                    premium = get_order_detail(interactive_access_token, app_order_id)

                    Put_Buy = round(premium, 2)
                    right = 'SELL'
                    entry_time = datetime.now().strftime('%H:%M:%S')
                    tgt = Put_Buy + target
                    sl = Put_Buy - stoploss  # Set initial SL
                    order = -1
                    orb = True
                    entry_time = datetime.strptime(entry_time, '%H:%M:%S')
                    entry_time = current_time.replace(hour=entry_time.hour, minute=entry_time.minute, second=entry_time.second, microsecond=0)
                    print(f"Put Buy at: {Put_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
                    logging.info(f"Put Buy at: {Put_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
            else:
                print("No Bearish Position")
                logging.info("No Bearish Position")
        print("ORB Checking")

    # Check for volume-based re-entry
    if order == 2 and time_1 < t(now.hour, now.minute) < time_2 and now.second == 0:
        year_to_expiry = (expiry_1 - datetime.now())/timedelta(days=1)/365
        
        time.sleep(1)
        print(f"Checking Volume-Based Re-entry at {now}")
        logging.info(f"Checking Volume-Based Re-entry at {now}")
        exchange_instrument_id = get_nifty_future_instrument_id(access_token, expiry_date)

        # Fetch OHLC data
        ohlc_data = get_nifty_future_ohlc_with_retry(access_token, exchange_instrument_id, start_time, end_time)        
        olhc = olhc_func(ohlc_data)

        avg_volume = olhc['volume'].ewm(span=10, min_periods=10).mean().iloc[-3]  
        last_row = olhc.iloc[-3]  
        vol = last_row['volume']
        factor = get_volume_factor(last_row['volume'], avg_volume)
               
        if factor:
            ltp = get_ltp_with_retry(access_token, "1", "26000")
            strike_price = round_to_nearest_50(ltp) 
            
            call_option_instrument_id = get_nifty_option_instrument(access_token, option_expiry_date, strike_price, "CE")            
            ohlc_call_data = get_nifty_option_ohlc_with_retry(access_token, call_option_instrument_id, start_time, end_time) 
            olhc_call = olhc_func(ohlc_call_data)

            put_option_instrument_id = get_nifty_option_instrument(access_token, option_expiry_date, strike_price, "PE")          
            olhc_put_data = get_nifty_option_ohlc_with_retry(access_token, put_option_instrument_id, start_time, end_time) 
            olhc_put = olhc_func(olhc_put_data)
            # Calculate volumes
            
            avg_volume_call = olhc_call['volume'].ewm(span=10, min_periods=10).mean().iloc[-3]
            avg_volume_put = olhc_put['volume'].ewm(span=10, min_periods=10).mean().iloc[-3]
            call_vol_candle = olhc_call.iloc[-3]
            put_vol_candle = olhc_put.iloc[-3]
            vol_call = call_vol_candle['volume'] 
            vol_put = put_vol_candle['volume']
            avg_call = vol_call / vol
            avg_put = vol_put / vol
        print(f"Volume-Based Re-entry with factor {factor} at {now}. Last close: {last_row['close']}, last volume: {last_row['volume']}, avg volume: {avg_volume}")

        if factor:
            update_volume_conditions(factor, last_row)
            
            candle_count = 0  
            while candle_count < 10:
                time.sleep(1)
                current_time = datetime.now()
                # Fetch updated OHLC data for real-time checking
                ohlc_data = get_nifty_future_ohlc_with_retry(access_token, exchange_instrument_id, start_time, end_time)
                olhc = olhc_func(ohlc_data)
          
                ohlc_call_data = get_nifty_option_ohlc_with_retry(access_token, call_option_instrument_id, start_time, end_time) 
                olhc_call = olhc_func(ohlc_call_data)
    
                ohlc_put_data = get_nifty_option_ohlc_with_retry(access_token, put_option_instrument_id, start_time, end_time)  
                olhc_put = olhc_func(ohlc_put_data)  
                
                latest_candle = olhc.iloc[-2]
                
                # Get previous 10 spot, call, and put data
                prev_spot_values = [get_ltp_with_retry(access_token, "1", "26000") for i in range(10)]
                prev_call_closes = [olhc_call.iloc[-(i+1)]['close'] for i in range(10)]
                prev_put_closes = [olhc_put.iloc[-(i+1)]['close'] for i in range(10)]
                
                # Calculate vega for the previous 10 candles using list comprehension
                call_vega_high = [
                    option_vega(prev_spot_values[i], strike_price, year_to_expiry, r, call_iv(prev_spot_values[i], strike_price, year_to_expiry, r, prev_call_closes[i], d), d)
                    for i in range(10)
                ]
                put_vega_high = [
                    option_vega(prev_spot_values[i], strike_price, year_to_expiry, r, put_iv(prev_spot_values[i], strike_price, year_to_expiry, r, prev_put_closes[i], d), d)
                    for i in range(10)
                ]
                latest_call_vega = call_vega_high[-1]
                latest_put_vega = put_vega_high[-1]
                print("Call Vega:  ", call_vega_high)
                print("Put Vega:  ", put_vega_high)
                print(avg_call, avg_put)
                # Check breakout conditions
                if avg_call >= 0.5:                    
                    if latest_candle['close'] > volume_high and olhc_call.iloc[-2]['close'] > call_vol_candle['high'] and latest_call_vega >= max(call_vega_high):                    
                        ltp = get_ltp_with_retry(access_token, "1", "26000")
                        strike_price = round_to_nearest_50(ltp)
                        premium = get_ltp_with_retry(access_token, exchange_segment, call_option_instrument_id) 
                        option_instrument_id = get_nifty_option_instrument(access_token, option_expiry_date, strike_price, "CE")                                       
                        # app_order_id = place_options_order(interactive_access_token, option_instrument_id, quantity, "BUY")
                        # premium = get_order_detail(interactive_access_token, app_order_id)    
                        Call_Buy = round(premium, 2)
                        right = 'Call_Buy'
                        entry_time = datetime.now().strftime('%H:%M:%S')
                        tgt = Call_Buy + target
                        sl = Call_Buy - stoploss
                        order = 1  
                        entry_time = datetime.strptime(entry_time, '%H:%M:%S')
                        #entry_time = current_time.replace(hour=entry_time.hour, minute=entry_time.minute, second=entry_time.second, microsecond=0)
                        print(f"{now} Volume-Based Call Buy at: {Call_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
                        logging.info(f"{now} Volume-Based Call Buy at: {Call_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
                        break
   
                elif avg_put >= 0.5:
                    if latest_candle['close'] < volume_low and olhc_put.iloc[-2]['close'] > put_vol_candle['high'] and latest_put_vega >= max(put_vega_high):
                        ltp = get_ltp_with_retry(access_token, "1", "26000")
                        strike_price = round_to_nearest_50(ltp)    
                        option_instrument_id = get_nifty_option_instrument(access_token, option_expiry_date, strike_price, "PE")               
                        # app_order_id = place_options_order(interactive_access_token, option_instrument_id, quantity, "BUY")
                        # premium = get_order_detail(interactive_access_token, app_order_id)
                        premium = get_ltp_with_retry(access_token, exchange_segment, put_option_instrument_id) 
                        Put_Buy = round(premium, 2)
                        right = 'Put_Buy'
                        entry_time = datetime.now().strftime('%H:%M:%S')
                        tgt = Put_Buy + target
                        sl = Put_Buy - stoploss
                        order = -1
                        entry_time = datetime.strptime(entry_time, '%H:%M:%S')
                        #entry_time = current_time.replace(hour=entry_time.hour, minute=entry_time.minute,second=entry_time.second, microsecond=0)
                        print(f"{now} Volume-Based Put Buy at: {Put_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
                        logging.info(f"{now} Volume-Based Put Buy at: {Put_Buy}, strike_price: {strike_price}, Target: {tgt}, Stoploss: {sl}")
                        break
                else:
                    break
                # Increment candle count
                candle_count += 1
                time.sleep(60)
                
    # Exit Conditions
    if order in [1, -1]:
        print(f"Checking exit conditions at {now}")
        logging.info(f"Checking exit conditions at {now}")
        time.sleep(5)
        current_time = datetime.now()

        time_difference = (current_time - entry_time).total_seconds() / 60
        #print(time_difference)
        exit_reason = ''
        if order == 1:    
            premium = get_ltp_with_retry(access_token, exchange_segment, option_instrument_id) 
            if premium >= sl + 15:
                sl = adjust_trailing_sl(premium, sl, factor, order)
                print(f"Stop Loss trailed. Premium: {premium}, New SL: {sl}")
                logging.info(f"Stop Loss trailed. Premium: {premium}, New SL: {sl}")

            if premium <= sl:
                #place_options_order(interactive_access_token, option_instrument_id, quantity, "SELL")
                exit_reason = 'Stoploss Hit'
            elif t(now.hour, now.minute) == t(15, 20):
                #place_options_order(interactive_access_token, option_instrument_id, quantity, "SELL")
                exit_reason = 'Market Close'

        elif order == -1:    
            premium = get_ltp_with_retry(access_token, exchange_segment, option_instrument_id)           
            if premium >= sl + 15:
                sl = adjust_trailing_sl(premium, sl, factor, order)
                print(f"Stop Loss trailed. Premium: {premium}, New SL: {sl}")
                logging.info(f"Stop Loss trailed. Premium: {premium}, New SL: {sl}")

            if premium <= sl:
                #place_options_order(interactive_access_token, option_instrument_id, quantity, "SELL")
                exit_reason = 'Stoploss Hit'
            elif t(now.hour, now.minute) == t(15, 20):
                #place_options_order(interactive_access_token, option_instrument_id, quantity, "SELL")
                exit_reason = 'Market Close'
        
        if exit_reason:
            print(f"{exit_reason}. Exiting position.")
            logging.info(f"{exit_reason}. Exiting position.")
            exit_time = datetime.now().strftime('%H:%M:%S')
            print(f"Exit Time: {exit_time}, strike_price: {strike_price}, LTP: {premium}")
            logging.info(f"Exit Time: {exit_time}, strike_price: {strike_price}, LTP: {premium}")
            
            # Calculate PNL
            pnl = (premium - Call_Buy) if order == 1 else (premium - Put_Buy)
            avg = vol / avg_volume
            # Log trade details to CSV
            log_trade_to_csv(today, entry_time, Call_Buy if order == 1 else Put_Buy, right, premium, avg ,avg_call if order == 1 else avg_put ,exit_time, strike_price, exit_reason, pnl)
            
            order = 2
            orb = False
        else:
            if orb:
                sl = adjust_trailing_sl_orb(premium, sl, order)
                print(f"Trailing Stoploss adjusted to {sl} at price {premium}")
                logging.info(f"Trailing Stoploss adjusted to {sl} at price {premium}")

    time.sleep(1)
