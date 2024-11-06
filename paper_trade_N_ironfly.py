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
import traceback

import warnings
warnings.filterwarnings("ignore")

time_1 = t(9,15)
time_2 = t(15,30)

expiry = '2024-11-07'
fut_expiry = '2024-11-28'

#qty = 25
order = 0
trades = 0

while True:
    now = datetime.now()
    if order == 0 and time_1 < t(datetime.now().time().hour, datetime.now().time().minute) < time_2 and now.second == 0 and now.minute % 5 == 0:
        time.sleep(1)
        today = datetime.now().strftime('%Y-%m-%d')
        yesterday = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        
        for j in range(0, 5):
            try:
                data = breeze.get_historical_data_v2(interval="5minute",
                                                     from_date= f"{yesterday}T00:00:00.000Z",
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
        
        
        olhc.ta.rsi(close='close', length=14, append=True)
        
        olhc['ATR'] = ta.atr(olhc['high'], olhc['low'], olhc['close'], length=14)
        
        olhc['UpMove'] = olhc['high'].diff()
        olhc['DownMove'] = -olhc['low'].diff()
        olhc['+DM'] = np.where((olhc['UpMove'] > olhc['DownMove']) & (olhc['UpMove'] > 0), olhc['UpMove'], 0)
        olhc['-DM'] = np.where((olhc['DownMove'] > olhc['UpMove']) & (olhc['DownMove'] > 0), olhc['DownMove'], 0)
        period = 14
        smoothed_period = 2 / (period + 1)
        olhc['+DI'] = olhc['+DM'].rolling(window=period).mean() * smoothed_period
        olhc['-DI'] = olhc['-DM'].rolling(window=period).mean() * smoothed_period        
        olhc['DX'] = 100 * np.abs((olhc['+DI'] - olhc['-DI']) / (olhc['+DI'] + olhc['-DI']))
        olhc['ADX'] = olhc['DX'].rolling(period).mean()        
        
        last_row = olhc.iloc[-1]
        second_last = olhc.iloc[-2]
        third_last = olhc.iloc[-3]
        fourth_last = olhc.iloc[-4]
        
        if (40<last_row['RSI_14']<60) and (last_row['ATR']<fourth_last['ATR']) and (last_row['ADX']<second_last['ADX']<third_last['ADX']<fourth_last['ADX']):
            order = 2
                
        else:
            print(now, 'no condition for ironfly')
    if order == 2:
        entry_time = datetime.now().strftime('%H:%M:%S')
        initial_point = 0
        SL = 0
        time.sleep(20)
        try:        
            nifty_spot = breeze.get_quotes(stock_code="NIFTY",
                                                   exchange_code="NSE",
                                                   product_type="cash",
                                                   right="others",
                                                   strike_price="0")

            if 'Success' in nifty_spot:
                nifty_spot = nifty_spot['Success']
                nifty_spot = pd.DataFrame(nifty_spot)
                nifty_spot = nifty_spot['ltp'][0]
            else:
                j+=1
        except Exception as e:
            j+=1
            print(traceback.print_exc(),"   ",e)
            time.sleep(5)
                
        atm_strike = round(nifty_spot / 100) * 100
        #otm_pe = atm_strike - 600
        #otm_ce = atm_strike + 600
        j=1
        for i in range(j):
            try:
                leg1 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product_type="options",
                                                                expiry_date=f'{expiry}T06:00:00.000Z',
                                                                right="call",
                                                                strike_price=atm_strike)
                if 'Success' in leg1:
                    leg1 = leg1['Success']
                    leg1 = pd.DataFrame(leg1)
                    # print("sdnsiks",leg1)
                    premium1 = float(leg1['ltp'][0])
                    break
                else:
                    j+=1
                    
            except:
                j+=1
                pass
                
        j=1
        for i in range(j):
            try:
                leg2 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product_type="options",
                                                                expiry_date=f'{expiry}T06:00:00.000Z',
                                                                right="put",
                                                                strike_price=atm_strike)
                if 'Success' in leg2:
                    leg2 = leg2['Success']
                    leg2 = pd.DataFrame(leg2)
                    premium2 = float(leg2['ltp'][0])     
                    premium_match = 0.10*premium1
                    break
                else:
                    j+=1
            except:
                j+=1
                pass

        if ((premium1+premium_match) > premium2) and (premium2 > (premium1-premium_match)):
            hedge_value = 0.1* (premium1+premium2)
            hedge_value = hedge_value / 2
            
            strikes = [atm_strike+50, atm_strike+100, atm_strike+150, atm_strike+200, atm_strike+250, atm_strike+300, atm_strike+350, atm_strike+400, atm_strike+450, atm_strike+500, atm_strike+550, atm_strike+600, atm_strike+650, atm_strike+700, atm_strike+750, atm_strike+800, atm_strike+850, atm_strike+900, atm_strike+950, atm_strike+1000, atm_strike+1050, atm_strike+1100]
            
            ltps = []


            for strike in strikes:
                try:
                    leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="call",
                                                            strike_price=strike)
                except Exception as e:
                    print(traceback.print_exc(),"   ",e)
        
                leg_df = leg['Success']
                leg_df = pd.DataFrame(leg_df)
                ltp_value = float(leg_df['ltp'])
                ltps.append({'strike_price': strike, 'ltp': ltp_value})
                    

            target_ltp = hedge_value
            closest_strike_ce = None
            min_diff = float('inf')

            for ltp_data in ltps:
                ltp = ltp_data['ltp']
                diff = abs(ltp - target_ltp)
                if diff < min_diff:
                    min_diff = diff
                    closest_strike_ce = ltp_data['strike_price']
                    
                
                
            leg4 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="call",
                                                        strike_price=closest_strike_ce)
            leg4 = leg4['Success']
            leg4 = pd.DataFrame(leg4)
            premium4 = float(leg4['ltp'])
            
            
            strikes = [atm_strike-50, atm_strike-100, atm_strike-150, atm_strike-200, atm_strike-250, atm_strike-300, atm_strike-350, atm_strike-400, atm_strike-450, atm_strike-500, atm_strike-550, atm_strike-600, atm_strike-650, atm_strike-700, atm_strike-750, atm_strike-800, atm_strike-850, atm_strike-900, atm_strike-950, atm_strike-1000, atm_strike-1050, atm_strike-1100]
            
            ltpsput = []


            for strike in strikes:
                try:
                    leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="put",
                                                            strike_price=strike)
        
                    leg_df = leg['Success']
                    leg_df = pd.DataFrame(leg_df)
                    ltp_value = float(leg_df['ltp'])
                    ltpsput.append({'strike_price': strike, 'ltp': ltp_value})
                except Exception as e:
                    print(e)
                    print(traceback.print_exception())
                    

            target_ltp = hedge_value
            closest_strike_pe = None
            min_diff = float('inf')

            for ltp_data in ltpsput:
                ltp = ltp_data['ltp']
                diff = abs(ltp - target_ltp)
                if diff < min_diff:
                    min_diff = diff
                    closest_strike_pe = ltp_data['strike_price']
                    
            j=1
            for i in range(j):    
                try:
                    leg3 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                                    exchange_code="NFO",
                                                                    product_type="options",
                                                                    expiry_date=f'{expiry}T06:00:00.000Z',
                                                                    right="put",
                                                                    strike_price=closest_strike_pe)
                    if 'Success' in leg4:
                        leg3 = leg3['Success']
                        leg3 = pd.DataFrame(leg3)
                        premium3 = float(leg3['ltp'])
                        break
                    else:
                        j+=1
                except Exception as e:
                    print(e)
                    print(traceback.print_exception())
                    j+=1
                    time.sleep(10)

                
            initial_combined_premium = (premium3 + premium4) - (premium1 + premium2)
            SL = -(0.1*initial_combined_premium)
            tsl = initial_combined_premium - SL
            print(now, 'iron_fly created')
            order = 1
                
        else:
            print('premium not mtached...')
            
            
            
    if order == 1:
        time.sleep(20)
        for i in range(0,5):
            try:
                leg1 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="call",
                                                            strike_price=atm_strike)
                leg1 = leg1['Success']
                leg1 = pd.DataFrame(leg1)
                
                leg1_cmp = float(leg1['ltp'])
            except:
                pass
        j=1
        for i in range(j):
            try:
                leg2 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="put",
                                                            strike_price=atm_strike)
                if 'Success' in leg4:
                    leg2 = leg2['Success']
                    leg2 = pd.DataFrame(leg2)
                    leg2_cmp = float(leg2['ltp'])
                    break
                else:
                    j+=1
            except Exception as e:
                print(e)
                print(traceback.print_exception())
                j+=1
                time.sleep(10)
        
        j=1
        for i in range(j): 
            try:
                leg3 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                            exchange_code="NFO",
                                                            product_type="options",
                                                            expiry_date=f'{expiry}T06:00:00.000Z',
                                                            right="put",
                                                            strike_price=closest_strike_pe)
                if 'Success' in leg3:
                    break
                else:
                    j+=1
            except Exception as e:
                print(e)
                print(traceback.print_exception())
                j+=1
                time.sleep(10)
        leg3 = leg3['Success']
        leg3 = pd.DataFrame(leg3)
        leg3_cmp = float(leg3['ltp'])
        j=1
        for i in range(j):
            try:
                leg4 = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                                exchange_code="NFO",
                                                                product_type="options",
                                                                expiry_date=f'{expiry}T06:00:00.000Z',
                                                                right="call",
                                                                strike_price=closest_strike_ce)
                if 'Success' in leg4:
                    leg4 = leg4['Success']
                    leg4 = pd.DataFrame(leg4)
                    leg4_cmp = float(leg4['ltp'])
                    break
                else:
                    j+=1
            except Exception as e:
                print(traceback.print_exception())
                j+=1
            
        
                
        cmp_combined_premium = (leg3_cmp + leg4_cmp) - (leg1_cmp + leg2_cmp)
            
        print('pnl is:', cmp_combined_premium - initial_combined_premium)
        
        if (cmp_combined_premium - initial_combined_premium) > initial_point :
            initial_point = (cmp_combined_premium - initial_combined_premium)
            tsl = cmp_combined_premium - SL
            
        if (cmp_combined_premium <= tsl) or (t(datetime.now().time().hour, datetime.now().time().minute) == t(15,29)) :
            order = 0
            trades += 1
            exit_time = datetime.now().strftime('%H:%M:%S')
            exit_premium = (leg3_cmp + leg4_cmp) - (leg1_cmp + leg2_cmp)
            pnl = round((exit_premium - initial_combined_premium), 2)
            
            csv_file = "Paper_trade_N_IronFly.csv"
            try:
                with open(csv_file, 'x', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow(['Date', 'Entry Time', 'ATM Strike', 'Combined Value', 'Exit time', 'Exit Value', 'PNL'])
            except FileExistsError:
                pass
                with open(csv_file, 'a', newline='') as file:
                    writer = csv.writer(file)
                    writer.writerow([today, entry_time, atm_strike, closest_strike_ce, closest_strike_pe, initial_combined_premium, exit_time, exit_premium, pnl])  
            print('all positions closed, pnl is:', pnl)
        else:
            print(now, 'no exit')
            
        
