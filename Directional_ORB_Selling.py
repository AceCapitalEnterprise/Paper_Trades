from breeze_connect import BreezeConnect
breeze = BreezeConnect(api_key="64mV026553514z5565%7S258@^4l3753")
import urllib
breeze.generate_session(api_secret="20286x551)P23443543722J0t28s90D3",
                        session_token="48941963")


import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta, time as t
import csv, re, time, math
import time

import warnings
warnings.filterwarnings("ignore")


time_1 = t(9, 30)
time_2 = t(15, 30)
order = 0
expiry = '2024-11-14'
fut_expiry = '2024-11-28'

SL = 5
positions = []
positions_df = pd.DataFrame(columns=['datetime', 'action', 'strike', 'premium', 'trailing_sl'])

def get_current_market_price(CE_or_PE, strike_price):
    global current_price
    for j in range (0, 5):
        try:
            current_price = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                   exchange_code="NFO",
                                                   product_type="options",
                                                   expiry_date=f'{expiry}T17:00:00.000Z',
                                                   right=CE_or_PE,
                                                   strike_price=strike_price
                                                  )
            current_price = current_price['Success']
            current_price = pd.DataFrame(current_price)
            current_price = float(current_price['ltp'][0])
            break
        except Exception as e:
            continue
    return current_price


def update_trailing_sl(positions_df):
    positions_to_exit = []

    for index, position in positions_df.iterrows():
        current_price = float(get_current_market_price(position['CE_or_PE'], position['strike']))

        
        if current_price >= position['trailing_sl']:
            positions_to_exit.append(index)
            time = datetime.now().strftime('%H:%M:%S')
            print('position exit')
            csv_file = "Directional_selling.csv"
            try:
                with open(csv_file, 'x', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
            except FileExistsError:
                pass
                with open(csv_file, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([today, time, position['strike'], position['CE_or_PE'], 'Buy', -(current_price)])

        elif current_price < position['trailing_sl'] - position['premium']:
            positions_df.at[index, 'trailing_sl'] = current_price + position['premium']
            
    for index in positions_to_exit:
        positions_df.drop(index, inplace=True)

    return positions_df



def closest_put_otm() :
    strikes = [atm_strike-50, atm_strike-100, atm_strike-150, atm_strike-200, atm_strike-250, atm_strike-300, atm_strike-350, atm_strike-400, atm_strike-450, atm_strike-500, atm_strike-550, atm_strike-600, atm_strike-650, atm_strike-700, atm_strike-750, atm_strike-800, atm_strike-850, atm_strike-900]
            
    ltps = []

    for strike in strikes:
        for j in range(0, 5):
            try:
                leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="put",
                                                        strike_price=strike)
                leg_df = leg['Success']
                leg_df = pd.DataFrame(leg_df)
                break
            except:
                pass
    
        ltp_value = float(leg_df['ltp'])
        ltps.append({'strike_price': strike, 'ltp': ltp_value})
                    

    target_ltp = 12
    closest_strike_pe = None
    min_diff = float('inf')

    for ltp_data in ltps:
        ltp = ltp_data['ltp']
        diff = abs(ltp - target_ltp)
        if diff < min_diff:
            min_diff = diff
            closest_strike_pe = ltp_data['strike_price']
            
    return closest_strike_pe



def closest_call_otm():
    strikes = [atm_strike+50, atm_strike+100, atm_strike+150, atm_strike+200, atm_strike+250, atm_strike+300, atm_strike+350, atm_strike+400, atm_strike+450, atm_strike+500, atm_strike+550, atm_strike+600, atm_strike+650, atm_strike+700, atm_strike+750, atm_strike+800, atm_strike+850, atm_strike+900]
            
    ltps = []


    for strike in strikes:
        for j in range(0, 5):
            try:
                leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="call",
                                                        strike_price=strike)
                leg_df = leg['Success']
                leg_df = pd.DataFrame(leg_df)
                break
            except:
                pass
    
        ltp_value = float(leg_df['ltp'])
        ltps.append({'strike_price': strike, 'ltp': ltp_value})
                    

    target_ltp = 12
    closest_strike_ce = None
    min_diff = float('inf')

    for ltp_data in ltps:
        ltp = ltp_data['ltp']
        diff = abs(ltp - target_ltp)
        if diff < min_diff:
            min_diff = diff
            closest_strike_ce = ltp_data['strike_price']
            
    return closest_strike_ce



def check_profit_target_and_add_position(positions_df):
    if not positions_df.empty:
        last_position = positions_df.iloc[-1]
        current_price = float(get_current_market_price(last_position['CE_or_PE'], last_position['strike']))

        target_price = last_position['premium'] * 0.75
        print(f"Current Price: {current_price}, Target Price: {target_price}")

        if (2.5) < current_price <= target_price:
            for j in range(5):
                try:
                    nifty_spot_response = breeze.get_quotes(stock_code="NIFTY", exchange_code="NSE",
                                                             expiry_date=f"{today}T06:00:00.000Z",
                                                             product_type="cash", right="others", strike_price="0")
                    nifty_spot = nifty_spot_response['Success']
                    nifty_spot = pd.DataFrame(nifty_spot)
                    nifty_spot_price = nifty_spot['ltp'][0]
                    print(f"Nifty Spot Price: {nifty_spot_price}")
                    break
                except Exception as e:
                    print(f"Error fetching Nifty spot: {e}")
                    continue

            atm = round(nifty_spot_price / 50) * 50

            if last_position['CE_or_PE'] == 'put':
                closest_strike_pe = closest_put_otm()
                for j in range(5):
                    try:
                        leg_response = breeze.get_option_chain_quotes(stock_code="NIFTY", exchange_code="NFO",
                                                                      product_type="options", expiry_date=f'{expiry}T06:00:00.000Z',
                                                                      right="put", strike_price=closest_strike_pe)
                        leg = leg_response['Success']
                        leg = pd.DataFrame(leg)
                        leg_price = float(leg['ltp'][0])
                        print(f"Leg Price for Put: {leg_price}")
                        break
                    except Exception as e:
                        print(f"Error fetching Put leg: {e}")
                        continue

                new_position = {
                    'datetime': now,
                    'action': 'sell',
                    'strike': closest_strike_pe,
                    'CE_or_PE': 'put',
                    'premium': leg_price,
                    'trailing_sl': 2*leg_price
                }

            else:
                closest_call_ce = closest_call_otm()
                for j in range(10):
                    try:
                        leg_response = breeze.get_option_chain_quotes(stock_code="NIFTY", exchange_code="NFO",
                                                                      product_type="options", expiry_date=f'{expiry}T06:00:00.000Z',
                                                                      right="call", strike_price=closest_call_ce)
                        leg = leg_response['Success']
                        leg = pd.DataFrame(leg)
                        leg_price = float(leg['ltp'][0])
                        print(f"Leg Price for Call: {leg_price}")
                        break
                    except Exception as e:
                        print(f"Error fetching Call leg: {e}")
                        continue

                new_position = {
                    'datetime': now,
                    'action': 'sell',
                    'strike': closest_call_ce,
                    'CE_or_PE': 'call',
                    'premium': leg_price,
                    'trailing_sl': 2*leg_price
                }

            with open(csv_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([today, datetime.now().strftime('%H:%M:%S'), new_position['strike'], new_position['CE_or_PE'], 'Sell', leg_price])

            # Create DataFrame for new position and concatenate
            new_position_df = pd.DataFrame([new_position])
            positions_df = pd.concat([positions_df, new_position_df], ignore_index=True)
            print(f"New Position Added: {new_position}")

        # Debug: Print the updated positions_df
        print(positions_df)

    return positions_df


while True:
    now = datetime.now()
    if t(9, 35)<=t(datetime.now().time().hour, datetime.now().time().minute)<t(9, 46) and now.second == 0 and positions_df.empty :
        time.sleep(2)
        today = datetime.now().strftime("%Y-%m-%d")
        #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        for j in range(0,5):
            try:
                data = breeze.get_historical_data_v2(interval="5minute",
                                                     from_date= f"{today}T00:00:00.000Z",
                                                     to_date= f"{today}T17:00:00.000Z",
                                                     stock_code="NIFTY",
                                                     exchange_code="NFO",
                                                     product_type="futures",
                                                     expiry_date=f'{fut_expiry}T07:00:00.000Z',
                                                     right="others",
                                                     strike_price="0")
                break
            except:
                pass
        
        olhc = data['Success']
        olhc = pd.DataFrame(olhc)
        olhc['datetime'] = pd.to_datetime(olhc['datetime'])
        olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                       (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
        candles_3 = olhc.iloc[-4:-1]
        resistance = candles_3['high'].max()
        support = candles_3['low'].min()
        last_row = olhc.iloc[-1]
        
        if last_row['close'] > resistance :
            atm_strike = round(last_row['close']/50) * 50
            closest_price_pe = closest_put_otm()

            for j in range(0,5):
                try:
                    option_data = breeze.get_historical_data_v2(interval="5minute",
                                                        from_date= f"{today}T07:00:00.000Z",
                                                        to_date= f"{today}T17:00:00.000Z",
                                                        stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f"{expiry}T07:00:00.000Z",
                                                        right="put",
                                                        strike_price=closest_strike_pe)
                    break
                except:
                    pass
            
            option_data = option_data['Success']
            option_data = pd.DataFrame(option_data)
            
            cand = option_data.iloc[-4:-1]
            sup = cand['low'].min()
            last = option_data.iloc[-1]
            
            if last['close'] <= sup :
                initial_point = 0
                order = 1
                time = datetime.now().strftime('%H:%M:%S')
                entry_premium = last['close']
                SL = entry_preium
                tsl = entry_premium + SL
                positions = []

                position = {
                    'datetime': time,
                    'action': 'sell',
                    'strike': closest_strike_pe,
                    'CE_or_PE': 'put',
                    'premium': entry_premium,
                    'trailing_sl': tsl
                }
                
                positions.append(position)
            
                print('SELL', closest_strike_pe, 'PUT at', entry_premium)
                
                csv_file = "Directional_selling.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, time, closest_strike_pe, 'PE', 'Sell', entry_premium])
                        
            else:
                print(now, 'No decay in option chart')
        else:
            print(now, 'Market in range')
                        
        if last_row['close'] < support :
            atm_strike = round(last_row['close']/50) * 50
            closest_price_ce = closest_call_otm()
            
            for j in range(0,5):
                try:
                    option_data = breeze.get_historical_data_v2(interval="5minute",
                                                        from_date= f"{today}T07:00:00.000Z",
                                                        to_date= f"{today}T17:00:00.000Z",
                                                        stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f"{expiry}T07:00:00.000Z",
                                                        right="call",
                                                        strike_price=closest_strike_ce)
                    break
                except:
                    pass
            
            option_data = option_data['Success']
            option_data = pd.DataFrame(option_data)
            
            cand = option_data.iloc[-4:-1]
            sup = cand['low'].min()
            last = option_data.iloc[-1]
            
            if last['close'] <= sup :
                initial_point = 0
                order = -1
                time = datetime.now().strftime('%H:%M:%S')
                entry_premium = last['close']
                SL = entry_preium
                tsl = entry_premium + SL
                positions = []

                position = {
                    'datetime': time,
                    'action': 'sell',
                    'strike': closest_strike_ce,
                    'CE_or_PE': 'call',
                    'premium': entry_premium,
                    'trailing_sl': tsl
                }
                
                positions.append(position)

                
                print('SELL', closest_strike_ce, 'CALL at', entry_premium)
                
                csv_file = "Directional_selling.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, time, closest_strike_ce, 'CE', 'Sell', entry_premium])
                        
            else:
                print(now, 'no decay in option chart')
        else:
            print(now, 'Market in range')
                        
                
                
    if t(9, 45)<t(datetime.now().time().hour, datetime.now().time().minute)<t(15, 20) and now.second == 0 and positions_df.empty :
        time.sleep(2)
        today = datetime.now().strftime("%Y-%m-%d")
        #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")

        for j in range(0,5):
            try:
                data = breeze.get_historical_data_v2(interval="5minute",
                                                     from_date= f"{today}T00:00:00.000Z",
                                                     to_date= f"{today}T17:00:00.000Z",
                                                     stock_code="NIFTY",
                                                     exchange_code="NFO",
                                                     product_type="futures",
                                                     expiry_date=f'{fut_expiry}T07:00:00.000Z',
                                                     right="others",
                                                     strike_price="0")
                break
            except:
                pass
        
        olhc = data['Success']
        olhc = pd.DataFrame(olhc)
        olhc['datetime'] = pd.to_datetime(olhc['datetime'])
        olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                       (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
        candles_3 = olhc.iloc[-7:-1]
        resistance = candles_3['high'].max()
        support = candles_3['low'].min()
        last_row = olhc.iloc[-1]
        
        if last_row['close'] > resistance :
            atm_strike = round(last_row['close']/50) * 50
            closest_strike_pe = closest_put_otm()
            

            option_data = breeze.get_historical_data_v2(interval="5minute",
                                                        from_date= f"{today}T07:00:00.000Z",
                                                        to_date= f"{today}T17:00:00.000Z",
                                                        stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f"{expiry}T07:00:00.000Z",
                                                        right="put",
                                                        strike_price=closest_strike_pe)
            option_data = option_data['Success']
            option_data = pd.DataFrame(option_data)
                  
            
            cand = option_data.iloc[-7:-1]
            sup = cand['low'].min()
            last = option_data.iloc[-1]
            
            if last['close'] <= sup :
                initial_point = 0
                order = 1
                time = datetime.now().strftime('%H:%M:%S')
                entry_premium = last['close']
                SL = entry_premium
                tsl = entry_premium + SL
                print('position excecuted')
                positions = []

                position = {
                    'datetime': time,
                    'action': 'sell',
                    'strike': closest_strike_pe,
                    'CE_or_PE': 'put',
                    'premium': entry_premium,
                    'trailing_sl': tsl
                }
                
                positions.append(position)
                positions_df = pd.DataFrame(positions)

            
                print('SELL', closest_strike_pe, 'PUT at', entry_premium)
                
                csv_file = "Directional_selling.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, time, closest_strike_pe, 'PE', 'Sell', entry_premium])
                        
            else:
                print(now, 'No decay in option chart')
        else:
            print(now, 'Market in range')
                        
        if last_row['close'] < support :
            atm_strike = round(last_row['close']/50) * 50
            closest_strike_ce = closest_call_otm()
            
            option_data = breeze.get_historical_data_v2(interval="5minute",
                                                                from_date= f"{today}T07:00:00.000Z",
                                                                to_date= f"{today}T17:00:00.000Z",
                                                                stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product_type="options",
                                                                expiry_date=f"{expiry}T07:00:00.000Z",
                                                                right="call",
                                                                strike_price=closest_strike_ce)
            option_data = option_data['Success']
            option_data = pd.DataFrame(option_data)
                  
            
            cand = option_data.iloc[-7:-1]
            sup = cand['low'].min()
            last = option_data.iloc[-1]
            
            if last['close'] <= sup :
                initial_point = 0
                order = -1
                time = datetime.now().strftime('%H:%M:%S')
                entry_premium = last['close']
                SL = entry_premium
                tsl = entry_premium + SL
                print('position executed')
                positions = []
                
                position = {
                    'datetime': time,
                    'action': 'sell',
                    'strike': closest_strike_ce,
                    'CE_or_PE': 'call',
                    'premium': entry_premium,
                    'trailing_sl': tsl
                }
                
                positions.append(position)

                positions_df = pd.DataFrame(positions)

                
                print('SELL', closest_strike_ce, 'CALL at', entry_premium)
                
                csv_file = "Directional_selling.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, time, closest_strike_ce, 'CE', 'Sell', entry_premium])
                        
            else:
                print(now, 'no decay in option chart')
        else:
            print(now, 'Market in range')
            
            
                    
    if not positions_df.empty:
        import time
        positions_df = update_trailing_sl(positions_df)
        positions_df = check_profit_target_and_add_position(positions_df)
        print(now)        
        time.sleep(20)


        
            
        
