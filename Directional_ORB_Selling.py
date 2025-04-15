from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import date, datetime, timedelta,time as t
import time as time_
import csv
import logging
import traceback
from tenacity import retry, stop_after_attempt, wait_exponential
import os
import warnings
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_selling_strategy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


# Initialize BreezeConnect
try:
    breeze = BreezeConnect(api_key="77%U3I71634^099gN232777%316Q~v4=")
    breeze.generate_session(
        api_secret="9331K77(I8_52JG2K73$5438q95772j@",
        session_token="51190178"
    )
    print("BreezeConnect initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize BreezeConnect: {str(e)}")
    print(f"Failed to initialize BreezeConnect: {str(e)}")
    exit(1)

# Constants
TIME_1 = datetime.strptime("09:15", "%H:%M").time()
TIME_2 = datetime.strptime("15:20", "%H:%M").time()
EXPIRY = '2025-04-17'
QTY = 225

atm_strike=None
open_position=None
# expiry = '2025-03-27'      
# expiry1 = '27-Mar-2025'
FUT_EXPIRY = '2025-04-24'
adding_pos = True
max_position = 3
spot_price=None
call_data = {}
put_data = {}


SL = 5
today = datetime.now().strftime("%Y-%m-%d")



one_tick=None
breeze.ws_connect()

# tick_data= {}

def on_ticks(ticks):
    # global one_tick
    global call_data,put_data,spot_price
    # print("-------------------------------------------------------------")
    # print(ticks)
    # if ticks['right']=='Others':
    #     spot_price=ticks['last']
    # else:
    if 'strike_price' in ticks:
        data = ticks['strike_price']+'_'+ticks['right']
        if ticks['right']=='Call':
            
            if data in call_data:
                call_data[data]=ticks['last']
            else:
                print(f'data:{data} not in call data:{call_data}')
        elif ticks['right']=='Put':
            if data in put_data:
                put_data[data]=ticks['last']
            else:
                print(f'data:{data} not in put data:{put_data}')
    else:
        spot_price=ticks['last']
    

breeze.on_ticks=on_ticks

def initiate_ws(CE_or_PE, strike_price):
    global call_data,put_data
    try:

        expiry1 = datetime.strptime(EXPIRY, '%Y-%m-%d')

        expiry1 = expiry1.strftime('%d-%b-%Y')
        if CE_or_PE!='others':
            leg = breeze.subscribe_feeds(exchange_code="NFO",
                                        stock_code="NIFTY",
                                        product_type="options",
                                        expiry_date=expiry1,
                                        right=CE_or_PE,
                                        strike_price=str(strike_price),
                                        get_exchange_quotes=True,
                                        get_market_depth=False)


            CE_or_PE = CE_or_PE.title()
            if CE_or_PE=='Call':
                call_data[f'{strike_price}_{CE_or_PE}']=''
                
            elif CE_or_PE=='Put':
                put_data[f'{strike_price}_{CE_or_PE}']=''
            print(leg)
        
        elif CE_or_PE=='others':
            leg = breeze.subscribe_feeds(exchange_code="NSE",
                                        stock_code="NIFTY",
                                        product_type="cash",
                                        expiry_date=expiry1,
                                        right=CE_or_PE,
                                        strike_price=str(strike_price),
                                        get_exchange_quotes=True,
                                        get_market_depth=False)
            print(leg)
    except Exception as e:
        logger.error(f"Activate websocket Error: {str(e)}")
        print(f"Activate websocket error: {str(e)}")
        

    # time.sleep(2)


def deactivate_ws(CE_or_PE,strike_price):

    try:

        expiry1 = datetime.strptime(EXPIRY, '%Y-%m-%d')

        expiry1 = expiry1.strftime('%d-%b-%Y')
        leg=breeze.unsubscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    expiry_date=expiry1,
                                    right=CE_or_PE,
                                    strike_price=str(strike_price),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
        data = strike_price+'_'+CE_or_PE.title()
        if data in call_data:
            call_data.pop(data)
        elif data in put_data:
            put_data.pop(data)
        else:
            print('Problem with',data)
        print(leg)
    except Exception as e:
        logger.error(f"Deactivate websocket Error: {str(e)}")
        print(f"Deactivate websocket error: {str(e)}")


def call_put_strikes(atm):
    global call_data,put_data

    try:

        call_strike=[atm + i for i in range(100, 1100, 50)]
        put_strike=[atm - i for i in range(100, 1100, 50)]

        for strike in call_strike:
            initiate_ws('call',strike)
            time_.sleep(2)

        for strike in put_strike:
            initiate_ws('put',strike)
            time_.sleep(2)
    except Exception as e:
        logger.error(f"Activate strike websocket Error: {str(e)}")
        print(f"Activate strike websocket error: {str(e)}")






def get_current_market_price(CE_or_PE, strike_price):
    global current_price,call_data,put_data
    print(f"Fetching price for: CE_or_PE={CE_or_PE}, strike_price={strike_price}")
    # print(f"Tick data: {tick_data.get(strike_price)}")

    data = str(strike_price)+'_'+CE_or_PE.title()
    # if f'{strike_price}_{CE_or_PE}' in tick_data and tick_data[f'{strike_price}_{CE_or_PE}']!='':
    #     tick_entry = tick_data[f'{strike_price}_{CE_or_PE}']
    #     if tick_entry.get('right') == CE_or_PE:
    #         current_price = tick_entry.get('last')  # Fetch the 'last' price
    #         return current_price
    if data in call_data and call_data[data]!='':
        current_price=call_data[data]
        return current_price
    elif data in put_data and put_data[data]!='':
        current_price=put_data[data]
        return current_price
    else:
        print('Problem with',data)

    return None

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def place_order(action, right, strike, qty):
    try:
        order_detail = breeze.place_order(
            stock_code="NIFTY",
            exchange_code="NFO",
            product="options",
            action=action,
            order_type="market",
            quantity=qty,
            price="",
            validity="day",
            disclosed_quantity="0",
            expiry_date=f'{EXPIRY}T06:00:00.000Z',
            right=right,
            strike_price=strike
        )
        time_.sleep(4)
        if order_detail.get('Success'):
            order_id = order_detail['Success']['order_id']
            trade_detail = breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
            if trade_detail.get('Success'):
                price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
                print(f"Order placed: {action} {right} at strike {strike} for {price}")
                return price
        raise ValueError("Order placement failed")
    except Exception as e:
        logger.error(f"Error placing {action} order for {right} {strike}: {str(e)}")
        print(f"Error placing {action} order for {right} {strike}: {str(e)}")
        raise


def update_trailing_sl(positions_df,path):
    positions_to_exit = []

    for index, position in positions_df.iterrows():
        current_price = get_current_market_price(position['CE_or_PE'], position['strike'])

        
        if current_price is not None and float(current_price) >= position['trailing_sl']:
            # order_detail = breeze.square_off(exchange_code="NFO",
            #                                     product="options",
            #                                     stock_code="NIFTY",
            #                                     expiry_date=f"{EXPIRY}T06:00:00.000Z",
            #                                     right=str(position['CE_or_PE']),
            #                                     strike_price=str(position['strike']),
            #                                     action="buy",
            #                                     order_type="market",
            #                                     validity="day",
            #                                     stoploss="0",
            #                                     quantity=QTY,
            #                                     price="0",
            #                                     trade_password="",
            #                                     disclosed_quantity="0")
            
            # time_.sleep(5)
            # order_detail = order_detail['Success']
            # order_detail = order_detail['order_id']
            # order_detail = breeze.get_trade_detail(exchange_code="NFO", order_id=order_detail)
            # order_detail = order_detail['Success']
            # order_detail = pd.DataFrame(order_detail)
            # sell_pe_price = order_detail['execution_price']
            # current_price = float(sell_pe_price[0])

            positions_to_exit.append(index)
            time = datetime.now().strftime('%H:%M:%S')
            print('position exit')
            write_to_csv([today, time, position['strike'], position['CE_or_PE'], 'Buy', -(current_price)])
            # deactivate_ws(position['CE_or_PE'], position['strike'])


        elif current_price is not None and float(current_price) < (position['trailing_sl']/2) :
            current_price=float(current_price)
            positions_df.at[index, 'trailing_sl'] = current_price * 2
            positions_df.to_csv(path,header=True,index=False)
            
    for index in positions_to_exit:
        positions_df.drop(index, inplace=True)
        positions_df.to_csv(path,header=True,index=False)

    return positions_df

def closest_put_otm() :
    global nearest_premium, adding_pos,put_data
    # strikes = [atm_strike - i for i in range(300, 1100, 50)] 
    ltps = []

    # for strike in strikes:
    #     try:
    #         ltp_value = leg_premium("put", strike)

    #         if ltp_value is not None:
    #             ltps.append({'strike_price': strike, 'ltp': ltp_value})
    #     except Exception as e:
    #         print(f"Error fetching LTP for strike {strike}: {e}")

    for key in put_data:
        try:
            strike=key.split('_')[0]
            ltp_value=put_data[key]
            if ltp_value is not None:
                ltps.append({'strike_price': strike, 'ltp': ltp_value})
        except Exception as e:
            print(f"Error fetching LTP for strike {strike}: {e}")

    if not ltps:
        print("No valid LTP data available.")
        closest_strike_pe = None
    else:
        target_ltp = 12
        nearest_premium = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['ltp']
        max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
        closest_strike_pe = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['strike_price']

    if max_ltp < 10:
        adding_pos = False
    else:
        adding_pos = True
    print("Closest strike with LTP near target:", closest_strike_pe, "LTP:", nearest_premium)
            
    return closest_strike_pe



def closest_call_otm():
    global nearest_premium, adding_pos,call_data
    # strikes = [atm_strike + i for i in range(300, 1100, 50)] 

    ltps = []

    # for strike in strikes:
    #     try:
    #         ltp_value = leg_premium("call", strike)
    #         if ltp_value is not None:
    #             ltps.append({'strike_price': strike, 'ltp': ltp_value})
    #     except Exception as e:
    #         print(f"Error fetching LTP for strike {strike}: {e}")

    for key in call_data:
        try:
            strike=key.split('_')[0]
            ltp_value=call_data[key]
            if ltp_value is not None:
                ltps.append({'strike_price': strike, 'ltp': ltp_value})
        except Exception as e:
            print(f"Error fetching LTP for strike {strike}: {e}")

    if not ltps:
        print("No valid LTP data available.")
        closest_strike_ce = None
    else:
        target_ltp = 12
        nearest_premium = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['ltp']
        max_ltp = max(ltps, key=lambda x: x['ltp'])['ltp']
        closest_strike_ce = min(ltps, key=lambda x: abs(x['ltp'] - target_ltp))['strike_price']

    if max_ltp < 10:
        adding_pos = False
    else:
        adding_pos = True

    print("Closest strike with LTP near target:", closest_strike_ce, "LTP:", nearest_premium)        
    return closest_strike_ce

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def nifty_spot():
    global spot_price
    try:
        
        if spot_price is not None:
            return float(spot_price)
        raise ValueError("No success response from API")
    except Exception as e:
        logging.error(f"Error fetching Nifty spot: {str(e)}")
        raise


def check_profit_target_and_add_position(positions_df, path, ce_or_pe):
    
    global adding_pos,open_position
    if not positions_df.empty:
        last_position = positions_df.iloc[-1]
        current_price = get_current_market_price(last_position['CE_or_PE'], last_position['strike'])
        target_price = last_position['premium'] * 0.75
        print(f"Current Price: {current_price}, Target Price: {target_price}")
    if not positions_df.empty and open_position < max_position and TIME_1<t(datetime.now().time().hour, datetime.now().time().minute) <= TIME_2 :
        if current_price is not None and (float(current_price) <= target_price) and adding_pos is True :
            current_price=float(current_price)
            
            # nifty_spot_response = breeze.get_quotes(stock_code="NIFTY", exchange_code="NSE",
            #                                                  expiry_date=f"{today}T06:00:00.000Z",
            #                                                  product_type="cash", right="others", strike_price="0")
            # time.sleep(1)
            # if nifty_spot_response is None:
            #     print(f"Error fetching Nifty spot")
            # nifty_spot = nifty_spot_response['Success']
            # nifty_spot = pd.DataFrame(nifty_spot)
            # nifty_spot_price = nifty_spot['ltp'][0]
            nifty_spot_price = nifty_spot()
            print(f"Nifty Spot Price: {nifty_spot_price}")
                    
            atm = round(nifty_spot_price / 50) * 50
            global atm_strike
            atm_strike=atm
            if ce_or_pe == "call":
                strike = closest_call_otm()
            elif ce_or_pe == "put":
                strike = closest_put_otm()

            if 10.5<nearest_premium<14 :
                
                # leg_price=place_order('sell',ce_or_pe,str(strike),QTY)
                leg_price=leg_premium(ce_or_pe,str(strike))
                print(f"Leg Price for Put: {leg_price}")


                new_position = {
                    'datetime': datetime.now().strftime('%H:%M:%S'),
                    'action': 'sell',
                    'strike': strike,
                    'CE_or_PE': ce_or_pe,
                    'premium': leg_price,
                    'trailing_sl': 2*leg_price
                }
    
                # initiate_ws(new_position['CE_or_PE'],strike)
                time_.sleep(4)

            
                new_position_df = pd.DataFrame([new_position])
                positions_df = pd.concat([positions_df, new_position_df], ignore_index=True)
                positions_df.to_csv(path,header=True,index=False)
                print(f"New Position Added: {new_position}")
                write_to_csv([today, datetime.now().strftime('%H:%M:%S'), new_position['strike'], new_position['CE_or_PE'], 'Sell', leg_price])
                
            
            
    print(positions_df)
    return positions_df


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def nifty_fut_historical():
    global today, FUT_EXPIRY
    data = breeze.get_historical_data_v2(interval="5minute",
                                            from_date= f"{today}T00:00:00.000Z",
                                            to_date= f"{today}T17:00:00.000Z",
                                            stock_code="NIFTY",
                                            exchange_code="NFO",
                                            product_type="futures",
                                            expiry_date=f'{FUT_EXPIRY}T07:00:00.000Z',
                                            right="others",
                                            strike_price="0")
    time_.sleep(0.5)
    if data is not None:
        olhc = data['Success']
        olhc = pd.DataFrame(olhc)
        olhc['datetime'] = pd.to_datetime(olhc['datetime'])
        olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                       (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        return olhc
    else:
        print("Error in fetching Nifty Futures historical data")
        nifty_fut_historical()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def option_historical(ce_or_pe, strike):
    global today, EXPIRY
    option_data = breeze.get_historical_data_v2(interval="5minute",
                                                        from_date= f"{today}T07:00:00.000Z",
                                                        to_date= f"{today}T17:00:00.000Z",
                                                        stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f"{EXPIRY}T07:00:00.000Z",
                                                        right=ce_or_pe,
                                                        strike_price=strike)
    time_.sleep(0.5)
    if option_data is not None:
        option_data = option_data['Success']
        option_data = pd.DataFrame(option_data)
        return option_data
    else:
        print("Error in fetching option historical data")
        option_historical(ce_or_pe, strike)


def write_to_csv(data):
    csv_file = "Directional_selling.csv"
    headers = ['Date', 'Time', 'Strike', 'CE/PE', 'Buy/Sell', 'Premium']
    try:
        
        try:
            with open(csv_file, 'x', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(headers)
                print("Created new CSV file")
        except FileExistsError:
            pass
        with open(csv_file, 'a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow(data)
            print(f"Trade data written to CSV: {data}")
    except Exception as e:
        logger.error(f"Error writing to CSV: {str(e)}")
        print(f"Error writing to CSV: {str(e)}")


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def leg_premium(ce_or_pe, strike):
    leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                exchange_code="NFO",
                                                product_type="options",
                                                expiry_date=f'{EXPIRY}T06:00:00.000Z',
                                                right=ce_or_pe,
                                                strike_price=strike)
    time_.sleep(0.1)
    if leg is not None:
        leg_df = leg['Success']
        leg_df = pd.DataFrame(leg_df)
        ltp_value = float(leg_df['ltp'])
        return ltp_value
    else:
        leg_premium(ce_or_pe, strike)
        print("Error in fetching option premium")





def main():
    global atm_strike,max_position,open_position
    try:
        initiate_ws('others','0')
        time_.sleep(1)
        path_ce="unclosed_positions_directional_ce.csv"
        if os.path.exists(path_ce):
            positions_df_ce=pd.read_csv(path_ce)
            
            # if not positions_df_ce.empty:
            #     for _,row in positions_df_ce.iterrows():
            #         initiate_ws(row['CE_or_PE'],row['strike'])
            #         time_.sleep(3)
            
        else:
            positions = []
            positions_df_ce = pd.DataFrame(columns=['datetime', 'action', 'strike', 'premium', 'trailing_sl'])
        print(positions_df_ce)

        path_pe="unclosed_positions_directional_pe.csv"
        if os.path.exists(path_pe):
            positions_df_pe=pd.read_csv(path_pe)
            # if not positions_df_pe.empty:
            #     for _,row in positions_df_pe.iterrows():
            #         initiate_ws(row['CE_or_PE'],row['strike'])
            #         time_.sleep(3)
            
        else:
            positions = []
            positions_df_pe = pd.DataFrame(columns=['datetime', 'action', 'strike', 'premium', 'trailing_sl'])
        print(positions_df_pe)
        
        atm_strike=round(nifty_spot()/50) * 50
        call_put_strikes(atm_strike)
        

        while True:
            open_position = len(positions_df_pe) + len(positions_df_ce)
            now = datetime.now()
            today = datetime.now().strftime("%Y-%m-%d")
            # now = datetime.now()
            current_time = now.time()
            if TIME_1 <= current_time <= TIME_2 and now.second == 0 and positions_df_pe.empty :
                try:
                    time_.sleep(2)
                    today = datetime.now().strftime("%Y-%m-%d")
                    #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
                    
                    olhc = nifty_fut_historical()
                    
                    candles_3 = olhc.iloc[-7:-1]
                    resistance = candles_3['high'].max()
                    support = candles_3['low'].min()
                    last_row = olhc.iloc[-1]
                    
                    if last_row['close'] > resistance :
                        atm_strike = round(nifty_spot()/50) * 50

                        closest_strike_pe = closest_put_otm()
                        
                        option_data = option_historical("put", closest_strike_pe)
                        cand = option_data.iloc[-7:-1]
                        sup = cand['low'].min()
                        last = option_data.iloc[-1]
                        
                        if last['close'] <= sup and 15 >= last['close'] >= 10 :
                            initial_point = 0
                            order = 1
                            time = datetime.now().strftime('%H:%M:%S')
                            
                            # entry_premium=place_order('sell','put',str(closest_strike_pe),QTY)
                            entry_premium=leg_premium('put',str(closest_strike_pe))

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
                            # initiate_ws('put',closest_strike_pe)
                            time_.sleep(3)
                            print('SELL', closest_strike_pe, 'PUT at', entry_premium)
                            write_to_csv([today, time, closest_strike_pe, 'put', 'Sell', entry_premium])
                            
                                    
                        else:
                            print(now, 'No decay in option chart')
                    else:
                        print(now, 'Market in range')
                except Exception as e:
                    logger.error(f"Error in PE execution logic: {str(e)}")
                    print(f"Error in PE execution: {str(e)}")
            # else:
            #     print("Outside trading hours or put's position df is not empty",put_data)
            if TIME_1 <= current_time <= TIME_2 and now.second == 0 and positions_df_ce.empty :

                try:
                    time_.sleep(2)
                    today = datetime.now().strftime("%Y-%m-%d")
                    #yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
                    
                    olhc = nifty_fut_historical()
                    candles_3 = olhc.iloc[-7:-1]
                    resistance = candles_3['high'].max()
                    support = candles_3['low'].min()
                    last_row = olhc.iloc[-1]
                    
                    if last_row['close'] < support :
                        atm_strike = round(nifty_spot()/50) * 50
                        closest_strike_ce = closest_call_otm()
                        
                        option_data = option_historical("call", closest_strike_ce)
                        
                        cand = option_data.iloc[-7:-1]
                        sup = cand['low'].min()
                        last = option_data.iloc[-1]
                        
                        if last['close'] <= sup and 15 >= last['close'] >= 10 :
                            initial_point = 0
                            order = -1
                            time = datetime.now().strftime('%H:%M:%S')
                            
                            # entry_premium=place_order('sell','call',str(closest_strike_ce),QTY)
                            entry_premium=leg_premium('call',str(closest_strike_ce))
                            
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
                            # initiate_ws('call',closest_strike_ce)
                            time_.sleep(4)
                            print('SELL', closest_strike_ce, 'CALL at', entry_premium)
                            write_to_csv([today, time, closest_strike_ce, 'call', 'Sell', entry_premium])

                                    
                        else:
                            print(now, 'no decay in option chart')
                    else:
                        print(now, 'Market in range')
                except Exception as e:
                        logger.error(f"Error in CE execution : {str(e)}")
                        print(f"Error in CE execution: {str(e)}")
            # else:
            #     print("Outside trading hours or call's position df is not empty",call_data)
                    
                                
            if not positions_df_pe.empty:
                
                print(now)
                positions_df_pe = update_trailing_sl(positions_df_pe,path_pe)
                positions_df_pe = check_profit_target_and_add_position(positions_df_pe, path_pe, "put")
                if now.time() > datetime.strptime("15:30", "%H:%M").time():
                    positions_df_pe.to_csv(path_pe,header=True,index=False)
                    print("All open Positions Saved and Market closed")
                time_.sleep(1) 


            if not positions_df_ce.empty:
                
                positions_df_ce = update_trailing_sl(positions_df_ce,path_ce)
                positions_df_ce = check_profit_target_and_add_position(positions_df_ce, path_ce, "call")
                if now.time() > datetime.strptime("15:30", "%H:%M").time():
                    positions_df_ce.to_csv(path_ce,header=True,index=False)
                    print("All open Positions Saved and Market closed")
                    quit() 
                print("____________________________________________________________________")
                time_.sleep(1)
    except Exception as e:
        logger.error(f"Main loop error: {str(e),traceback.print_exec()}")
        print(f"Main loop error: {str(e),traceback.print_exec()}")
        time_.sleep(60)


if __name__ == "__main__":
    main()
        
