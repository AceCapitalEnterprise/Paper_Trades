from breeze_connect import BreezeConnect
breeze = BreezeConnect(api_key="d96783Qp368558*55FI36Z24W0ET39Lf")
breeze.generate_session(api_secret="58836g597W4l8977h7~%eX9967^807x3",
                        session_token="50698898")

import numpy as np
import pandas as pd
from datetime import datetime, date, timedelta, time as t 
import csv, re, time, math
import time as time_
import os

import warnings
warnings.filterwarnings("ignore")


time_1 = t(9, 30)
time_2 = t(15, 30)
order = 0
atm_strike=None
expiry = '2025-02-27'      
expiry1 = '27-Feb-2025'
fut_expiry = '2025-02-27'
adding_pos = True

SL = 5
today = datetime.now().strftime("%Y-%m-%d")
one_tick=None


breeze.ws_connect()

tick_data= {}
def on_ticks(ticks):
    global tick_data
    # print(ticks)
    # one_tick=ticks
    key=str(ticks['strike_price'])+'_'+str(ticks['right'])
    if key in tick_data:
        tick_data[key] = ticks
        # print(tick_data)
    # print("-----------------------------------------------")
    
breeze.on_ticks=on_ticks

def initiate_ws(CE_or_PE, strike_price):
    global tick_data
    leg = breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=expiry1,
                                right=CE_or_PE,
                                strike_price=str(strike_price),
                                get_exchange_quotes=True,
                                get_market_depth=False)
    CE_or_PE = CE_or_PE.title()
    tick_data[f'{strike_price}_{CE_or_PE}']=''
    print(leg)
    # time.sleep(2)


def deactivate_ws(CE_or_PE,strike_price):
    leg=breeze.unsubscribe_feeds(exchange_code="NFO",
                                 stock_code="NIFTY",
                                 product_type="options",
                                 expiry_date=expiry1,
                                 right=CE_or_PE,
                                 strike_price=str(strike_price),
                                 get_exchange_quotes=True,
                                 get_market_depth=False)
    CE_or_PE = CE_or_PE.title()
    if f'{strike_price}_{CE_or_PE}' in tick_data:
        tick_data.pop(f'{strike_price}_{CE_or_PE}')
    print(leg)


path_ce="unclosed_positions_directional_ce.csv"
if os.path.exists(path_ce):
    positions_df_ce=pd.read_csv(path_ce)
    if not positions_df_ce.empty:
        for _,row in positions_df_ce.iterrows():
            initiate_ws(row['CE_or_PE'],row['strike'])
            time_.sleep(3)
    
else:
    positions = []
    positions_df_ce = pd.DataFrame(columns=['datetime', 'action', 'strike', 'premium', 'trailing_sl'])

path_pe="unclosed_positions_directional_pe.csv"
if os.path.exists(path_pe):
    positions_df_pe=pd.read_csv(path_pe)
    if not positions_df_pe.empty:
        for _,row in positions_df_pe.iterrows():
            initiate_ws(row['CE_or_PE'],row['strike'])
            time_.sleep(3)
    
else:
    positions = []
    positions_df_pe = pd.DataFrame(columns=['datetime', 'action', 'strike', 'premium', 'trailing_sl'])




def get_current_market_price(CE_or_PE, strike_price):
    global current_price,tick_data
    print(f"Fetching price for: CE_or_PE={CE_or_PE}, strike_price={strike_price}")
    # print(f"Tick data: {tick_data.get(strike_price)}")

    CE_or_PE = CE_or_PE.title()
    if f'{strike_price}_{CE_or_PE}' in tick_data and tick_data[f'{strike_price}_{CE_or_PE}']!='':
        tick_entry = tick_data[f'{strike_price}_{CE_or_PE}']
        # CE_or_PE = CE_or_PE.title()
        # Check if the tick data contains the correct option type (CE/PE)
        if tick_entry.get('right') == CE_or_PE:
            current_price = tick_entry.get('last')  # Fetch the 'last' price
            return current_price
    return None

def exit_positions_expiry(positions_df,path):
    positions_to_exit = []

    for index, position in positions_df.iterrows():
        current_price = get_current_market_price(position['CE_or_PE'], position['strike'])

        
        if current_price is not None:
            current_price=float(current_price)
            positions_to_exit.append(index)
            time = datetime.now().strftime('%H:%M:%S')
            print('position exit')
            deactivate_ws(position['CE_or_PE'], position['strike'])
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

        # elif current_price is not None and float(current_price) < position['trailing_sl'] - position['premium']:
        #     current_price=float(current_price)
        #     positions_df.at[index, 'trailing_sl'] = current_price + position['premium']
            
    for index in positions_to_exit:
        positions_df.drop(index, inplace=True)
        positions_df.to_csv(path,header=True,index=False)

    return positions_df



def update_trailing_sl(positions_df,path):
    positions_to_exit = []

    for index, position in positions_df.iterrows():
        current_price = get_current_market_price(position['CE_or_PE'], position['strike'])

        
        if current_price is not None and float(current_price) >= position['trailing_sl']:
            current_price=float(current_price)
            positions_to_exit.append(index)
            time = datetime.now().strftime('%H:%M:%S')
            print('position exit')
            deactivate_ws(position['CE_or_PE'], position['strike'])
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

        elif current_price is not None and float(current_price) < position['trailing_sl'] - position['premium']:
            current_price=float(current_price)
            positions_df.at[index, 'trailing_sl'] = current_price + position['premium']
            
    for index in positions_to_exit:
        positions_df.drop(index, inplace=True)
        positions_df.to_csv(path,header=True,index=False)

    return positions_df


# def leg_option_data(right,expiry2,strike_price):
#     # if retries <= 0:
#     #     raise Exception("Failed to fetch data after multiple retries.")
    
#     try:
#         data = breeze.get_option_chain_quotes(stock_code="NIFTY",
#                                                         exchange_code="NFO",
#                                                         product_type="options",
#                                                         expiry_date=f'{expiry2}T06:00:00.000Z',
#                                                         right=right,
#                                                         strike_price=strike_price)
        
#         if data['Status'] == 200:
#             data = data['Success']
#             return data
#         else:
#             print("Retrying due to non-200 status...")
#             return leg_option_data(right,expiry2,strike_price)

    # except Exception as e:
    #     print(f"Error occurred: {e}. Retrying...")
    #     time.sleep(1)
    #     return leg_option_data(right,expiry2,strike_price)



def closest_put_otm() :
    strikes = [atm_strike-50, atm_strike-100, atm_strike-150, atm_strike-200, atm_strike-250, atm_strike-300, atm_strike-350, atm_strike-400, atm_strike-450, atm_strike-500, atm_strike-550, atm_strike-600, atm_strike-650, atm_strike-700, atm_strike-750, atm_strike-800, atm_strike-850, atm_strike-900,atm_strike-950,atm_strike-1000,atm_strike-1050]
            
    ltps = []
    
    for strike in strikes:
        check = True
        while check:
            try:
                leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product_type="options",
                                                                expiry_date=f'{expiry}T06:00:00.000Z',
                                                                right="put",
                                                                strike_price=strike)
                        # print('leg',leg)
                time.sleep(0.5)
                if leg['Status']==200:
                  leg_df = leg['Success']
                break

            except Exception as e:
                print(f"Error while fetching leg data {e}")
                # time.sleep(1)
    
    #   leg_df = leg_option_data(right="call",strike_price=strike,expiry2=expiry)
        leg_df = pd.DataFrame(leg_df)
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
    strikes = [atm_strike+50, atm_strike+100, atm_strike+150, atm_strike+200, atm_strike+250, atm_strike+300, atm_strike+350, atm_strike+400, atm_strike+450, atm_strike+500, atm_strike+550, atm_strike+600, atm_strike+650, atm_strike+700, atm_strike+750, atm_strike+800, atm_strike+850, atm_strike+900,atm_strike+950,atm_strike+1000,atm_strike+1050]
            
    ltps = []


    for strike in strikes:
      
      # leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
      #                                           exchange_code="NFO",
      #                                           product_type="options",
      #                                           expiry_date=f'{expiry}T06:00:00.000Z',
      #                                           right="call",
      #                                           strike_price=strike)
        check = True
        while check:
            try:
                leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product_type="options",
                                                                expiry_date=f'{expiry}T06:00:00.000Z',
                                                                right="call",
                                                                strike_price=strike)
                        # print('leg',leg)
                time.sleep(0.5)
                if leg['Status']==200:
                  leg_df = leg['Success']
                break

            except Exception as e:
                print(f"Error while fetching leg data {e}")
                
    
    #   leg_df = leg_option_data(right="call",strike_price=strike,expiry2=expiry)
        leg_df = pd.DataFrame(leg_df)
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



def check_profit_target_and_add_position(positions_df,path):
    
    global adding_pos  
    if not positions_df.empty:
        last_position = positions_df.iloc[-1]
        # initiate_ws(last_position['CE_or_PE'],last_position['strike'])
        
        current_price = get_current_market_price(last_position['CE_or_PE'], last_position['strike'])

        target_price = last_position['premium'] * 0.75
        print(f"Current Price: {current_price}, Target Price: {target_price}")

        if current_price is not None and (float(current_price) <= target_price) and adding_pos is True :
            current_price=float(current_price)
            i=5
            for j in range(i):
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
                    time_.sleep(0.2)
                    i+=1
                    pass

            atm = round(nifty_spot_price / 50) * 50
            global atm_strike
            atm_strike=atm

            if last_position['CE_or_PE'] == 'put':
                
                closest_strike_pe = closest_put_otm()
                for i in range(5):
                  leg_response = breeze.get_option_chain_quotes(stock_code="NIFTY", exchange_code="NFO",
                                                              product_type="options", expiry_date=f'{expiry}T06:00:00.000Z',
                                                              right="put", strike_price=closest_strike_pe)
                  time.sleep(2)
                  print("leg_response",leg_response)
                  if leg_response['Status']==200:
                    leg = leg_response['Success']
                    break
                  time.sleep(1.5)
                leg = pd.DataFrame(leg)
                leg_price = float(leg['ltp'][0])
                print(f"Leg Price for Put: {leg_price}")


                new_position = {
                    'datetime': now,
                    'action': 'sell',
                    'strike': closest_strike_pe,
                    'CE_or_PE': 'put',
                    'premium': leg_price,
                    'trailing_sl': 2*leg_price
                }
                if leg_price>8:
                  initiate_ws(new_position['CE_or_PE'],closest_strike_pe)
                  time_.sleep(4)

            else:
                closest_call_ce = closest_call_otm()
                for i in range(5):
                  
                  leg_response = breeze.get_option_chain_quotes(stock_code="NIFTY", exchange_code="NFO",
                                                                product_type="options", expiry_date=f'{expiry}T06:00:00.000Z',
                                                                right="call", strike_price=closest_call_ce)
                  time.sleep(2)
                  print("leg_response",leg_response)
                  if leg_response['Status']==200:
                    leg = leg_response['Success']
                    break
                  time.sleep(1.5)
                leg = pd.DataFrame(leg)
                leg_price = float(leg['ltp'][0])
                print(f"Leg Price for Call: {leg_price}")
                
                new_position = {
                    'datetime': now,
                    'action': 'sell',
                    'strike': closest_call_ce,
                    'CE_or_PE': 'call',
                    'premium': leg_price,
                    'trailing_sl': 2*leg_price
                }
                if leg_price>10:
                  initiate_ws(new_position['CE_or_PE'],closest_call_ce)
                  time_.sleep(4)
             
            # Create DataFrame for new position and concatenate
            if leg_price>8:
              new_position_df = pd.DataFrame([new_position])
              positions_df = pd.concat([positions_df, new_position_df], ignore_index=True)
              positions_df.to_csv(path,header=True,index=False)
              print(f"New Position Added: {new_position}")
              csv_file='Directional_selling.csv'
              with open(csv_file, 'a', newline='') as file:
                  writer = csv.writer(file)
                  writer.writerow([today, datetime.now().strftime('%H:%M:%S'), new_position['strike'], new_position['CE_or_PE'], 'Sell', leg_price])
            else:
              adding_pos = False
              print("Premium is less than 8")
          

        # Debug: Print the updated positions_df
        print(positions_df)
        # deactivate_ws(last_position['CE_or_PE'],last_position['strike'])
    return positions_df




while True:  
    now = datetime.now()
    today = datetime.now().strftime("%Y-%m-%d")
    if t(9, 45)<t(datetime.now().time().hour, datetime.now().time().minute)<t(15, 20) and now.second == 0 and positions_df_pe.empty :
        time_.sleep(2)
        today = datetime.now().strftime("%Y-%m-%d")
        #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        i=5
        for j in range(i):
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
                olhc = data['Success']
                break
            except:
                i+=1
                time_.sleep(0.2)
                # pass
        
        
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
            
            if last['close'] <= sup and last['close'] >= 10 :
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
                positions_df_pe = pd.DataFrame(positions)
                positions_df_pe.to_csv(path_pe,header=True,index=False)
                initiate_ws('put',closest_strike_pe)
                time_.sleep(3)
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
                        writer.writerow([today, time, closest_strike_pe, 'put', 'Sell', entry_premium])
                        
            else:
                print(now, 'No decay in option chart')
        else:
            print(now, 'Market in range')
    if t(9, 45)<t(datetime.now().time().hour, datetime.now().time().minute)<t(15, 20) and now.second == 0 and positions_df_ce.empty :
        time_.sleep(2)
        today = datetime.now().strftime("%Y-%m-%d")
        #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        i=5
        for j in range(i):
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
                olhc = data['Success']
                break
            except:
                i+=1
                time_.sleep(0.2)
                # pass
        
        
        olhc = pd.DataFrame(olhc)
        olhc['datetime'] = pd.to_datetime(olhc['datetime'])
        olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                       (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
        candles_3 = olhc.iloc[-7:-1]
        resistance = candles_3['high'].max()
        support = candles_3['low'].min()
        last_row = olhc.iloc[-1]
        
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
            
            if last['close'] <= sup and last['close'] >= 10 :
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

                positions_df_ce = pd.DataFrame(positions)
                positions_df_ce.to_csv(path_ce,header=True,index=False)
                initiate_ws('call',closest_strike_ce)
                time_.sleep(4)
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
                        writer.writerow([today, time, closest_strike_ce, 'call', 'Sell', entry_premium])
                        
            else:
                print(now, 'no decay in option chart')
        else:
            print(now, 'Market in range')
    if expiry==today and t(datetime.now().time().hour, datetime.now().time().minute)>=t(15, 28):
      positions_df_ce=exit_positions_expiry(positions_df_ce,path_ce)
      positions_df_pe=exit_positions_expiry(positions_df_pe,path_pe)
      
            
            
                    
    if not positions_df_pe.empty:
        import time,os
        positions_df_pe = update_trailing_sl(positions_df_pe,path_pe)
        # print(positions_df_pe)
        positions_df_pe = check_profit_target_and_add_position(positions_df_pe,path_pe)
        if now.time() >= t(15, 30):
            path_pe="unclosed_positions_directional_pe.csv"
            # if os.path.exists(path):

            #     positions_df.to_csv(csv_file,header=False,mode='a',index=False)
            # else:
            positions_df_pe.to_csv(path_pe,header=True,index=False)
            print("All open Positions Saved and Market closed")
            
        time_.sleep(1)
        print(now)  


    if not positions_df_ce.empty:
        import time,os
        positions_df_ce = update_trailing_sl(positions_df_ce,path_ce)
        # print(positions_df_ce)
        positions_df_ce = check_profit_target_and_add_position(positions_df_ce,path_ce)
        if now.time() >= t(15, 30):
            path_ce="unclosed_positions_directional_ce.csv"
            # if os.path.exists(path):

            #     positions_df.to_csv(csv_file,header=False,mode='a',index=False)
            # else:
            positions_df_ce.to_csv(path_ce,header=True,index=False)
            print("All open Positions Saved and Market closed")
            quit()
        time_.sleep(1)
        print(now)  
        
