from breeze_connect import BreezeConnect
import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import date, datetime, timedelta
import time
import csv
import logging
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('trading_macd_strategy.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize BreezeConnect
try:
    breeze = BreezeConnect(api_key="77%U3I71634^099gN232777%316Q~v4=")
    breeze.generate_session(
        api_secret="9331K77(I8_52JG2K73$5438q95772j@",
        session_token="51135689"
    )
    print("BreezeConnect initialized successfully")
except Exception as e:
    logger.error(f"Failed to initialize BreezeConnect: {str(e)}")
    print(f"Failed to initialize BreezeConnect: {str(e)}")
    exit(1)

# Constants
TIME_1 = datetime.strptime("09:15", "%H:%M").time()
TIME_2 = datetime.strptime("15:20", "%H:%M").time()
EXPIRY = '2025-04-03'
QTY = 75

# Global variables
order = 0  # For CE
# order2 = 0  # For PE
sl = 0    # For CE
# sl2 = 0   # For PE
spot_price=None
tick_data={}
breeze.ws_connect()
expiry1 = datetime.strptime(EXPIRY, '%Y-%m-%d')

expiry1 = expiry1.strftime('%d-%b-%Y')




def on_ticks(ticks):
    # global one_tick
    global tick_data,spot_price
    # print("-------------------------------------------------------------")
    # print(ticks)
    # if ticks['right']=='Others':
    #     spot_price=ticks['last']
    # else:
    if 'strike_price' in ticks:
        data = ticks['strike_price']+'_'+ticks['right']
        if data in tick_data:
            tick_data[data]=ticks['last']
    else:
        spot_price=ticks['last']

        # print(ticks)


breeze.on_ticks=on_ticks

def initiate_ws( atm,right=''):
    global tick_data
    if right=='call':    
        # print("hello")
        leg=breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                # expiry_date=f'{expiry}T06:00:00.000Z',
                                expiry_date=expiry1,
                                right="call",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        tick_data[str(atm)+'_Call']=''
    # print(leg)
    elif right=='put':
        leg2=breeze.subscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=expiry1,
                                right="put",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        tick_data[str(atm)+'_Put']=''
        # print(leg2)
    elif right=='others':
        leg3=breeze.subscribe_feeds(exchange_code="NSE",
                                stock_code="NIFTY",
                                product_type="CASH",
                                expiry_date=expiry1,
                                right="others",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)

def deactivate_ws( atm,right=''):
    global tick_data
    if right=='call':    
        breeze.unsubscribe_feeds(exchange_code="NFO",
                            stock_code="NIFTY",
                            product_type="options",
                            expiry_date=expiry1,
                            right="call",
                            strike_price=str(atm),
                            get_exchange_quotes=True,
                            get_market_depth=False)
        tick_data.pop(str(atm)+'_Call')
    elif right=='put':
        breeze.unsubscribe_feeds(exchange_code="NFO",
                            stock_code="NIFTY",
                            product_type="options",
                            expiry_date=expiry1,
                            right="put",
                            strike_price=str(atm),
                            get_exchange_quotes=True,
                            get_market_depth=False)
        tick_data.pop(str(atm)+'_Put')

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def nifty_spot():
    try:
        
        if spot_price is not None:
            return float(spot_price)
        raise ValueError("No success response from API")
    except Exception as e:
        logging.error(f"Error fetching Nifty spot: {str(e)}")
        raise


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=4, max=10))
def fetch_historical( yesterday, today):
    try:
        option_data = breeze.get_historical_data_v2(
            interval="1minute",
            from_date=f"{yesterday}T07:00:00.000Z",
            to_date=f"{today}T17:00:00.000Z",
            stock_code="NIFTY",
            exchange_code="NSE",
            product_type="cash"
        )
        time.sleep(4)
        if option_data.get('Success'):
            df = pd.DataFrame(option_data['Success'])
            df['datetime'] = pd.to_datetime(df['datetime'])
            df = df[(df['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                        (df['datetime'].dt.time <= pd.to_datetime('15:29').time())]
            
            df['12_EMA'] = df['close'].ewm(span=12, adjust=False).mean()
            df['26_EMA'] = df['close'].ewm(span=26, adjust=False).mean()
            df['MACD_Line'] = df['12_EMA'] - df['26_EMA']
            df['Signal_Line'] = df['MACD_Line'].ewm(span=9, adjust=False).mean()
            df['MACD_Histogram'] = df['MACD_Line'] - df['Signal_Line']
            df['MACD'] = df['MACD_Line']
            
            df['close'] = pd.to_numeric(df['close'])
            df.ta.rsi(close='close', length=14, append=True)
            print(f"Historical data fetched ")

            return df
        raise ValueError("No success response from API")
    except Exception as e:
        logger.error(f"Error fetching historical data : {str(e)}")
        print(f"Error fetching historical data : {str(e)}")
        raise


def place_order(action, right, strike, qty):
    try:
        # order_detail = breeze.place_order(
        #     stock_code="NIFTY",
        #     exchange_code="NFO",
        #     product="options",
        #     action=action,
        #     order_type="market",
        #     quantity=qty,
        #     price="",
        #     validity="day",
        #     disclosed_quantity="0",
        #     expiry_date=f'{EXPIRY}T06:00:00.000Z',
        #     right=right,
        #     strike_price=strike
        # )
        # time.sleep(4)
        # if order_detail.get('Success'):
        #     order_id = order_detail['Success']['order_id']
        #     trade_detail = breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
        #     if trade_detail.get('Success'):
        #         price = float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
        #         print(f"Order placed: {action} {right} at strike {strike} for {price}")
        #         return price
        # raise ValueError("Order placement failed")
        leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                    exchange_code="NFO",
                                                    product_type="options",
                                                    expiry_date=f'{EXPIRY}T06:00:00.000Z',
                                                    right=right,
                                                    strike_price=strike)
        time.sleep(0.1)
        if leg is not None:
            leg_df = leg['Success']
            leg_df = pd.DataFrame(leg_df)
            ltp_value = float(leg_df['ltp'])
            return ltp_value
        else:
            place_order(action,right, strike,QTY)
            print("Error in fetching option premium")
    except Exception as e:
        logger.error(f"Error placing {action} order for {right} {strike}: {str(e)}")
        print(f"Error placing {action} order for {right} {strike}: {str(e)}")
        raise

def write_to_csv(data):
    csv_file = "macd(live)_RPS.csv"
    headers = ['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium', 
              'Exit Time', 'Exit premium', 'PnL', 'Quantity']
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

def main():
    global order, sl,tick_data
    initiate_ws('0','others')
    while True:
        try:
            now = datetime.now()
            current_time = now.time()
            
            if TIME_1 <= current_time <= TIME_2:
                today = now.strftime('%Y-%m-%d')
                yesterday = (now - timedelta(days=5)).strftime('%Y-%m-%d')
                
                # CE Entry Logic
                if order == 0 and now.second == 0:
                    try:
                        # nifty = nifty_spot()
                        # atm = round(nifty / 50) * 50
                        # ce_otm = atm + 100
                        # print(f"CE OTM Strike: {ce_otm}")
                        
                        # ce_option = option_historical("call", ce_otm, yesterday, today)
                        # ce_option.ta.rsi(close='close', length=14, append=True)
                        # supertrend = ta.supertrend(ce_option['high'], ce_option['low'], 
                        #                         ce_option['close'], length=10, multiplier=2)
                        # ce_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                        # ce_option['volume_avg'] = ce_option['volume'].rolling(window=5).mean()
                        # ce_option['volume_check'] = (ce_option['volume'] > 1.5 * ce_option['volume_avg']).astype(int)
                        
                        # last_row = ce_option.iloc[-1]
                        # rsi_condition = last_row['RSI_14'] > 70
                        # supertrend_condition = last_row['supertrend'] == 1
                        # volume_condition = last_row['volume_check'] == 1
                        # print(f"CE Conditions - RSI: {last_row['RSI_14']} (>70: {rsi_condition}), "
                        #       f"Supertrend: {last_row['supertrend']} (1: {supertrend_condition}), "
                        #       f"Volume Check: {last_row['volume_check']} (1: {volume_condition})")

                        olhc=fetch_historical(yesterday,today)
                        last_row = olhc.iloc[-1]
                        second_last = olhc.iloc[-2]
                        third_last = olhc.iloc[-3]
                        rsi_ce_condition= (last_row['RSI_14'] > 70) and (second_last['RSI_14'] > 70)
                        macd_ce_condition= (last_row['MACD']>second_last['MACD'] and second_last['MACD']>third_last['MACD'])
                        rsi_pe_condition=(last_row['RSI_14']<30 and second_last['RSI_14']<30)
                        macd_pe_condition=(last_row['MACD']<second_last['MACD'] and second_last['MACD']<third_last['MACD'])
                        # last_row['RSI_14']>70 and second_last['RSI_14']>70 and last_row['MACD']>second_last['MACD'] and second_last['MACD']>third_last['MACD']
                        
                        if rsi_ce_condition and macd_ce_condition:
                            nifty = nifty_spot()
                            atm = round(nifty / 50) * 50
                            entry_time = now.strftime('%H:%M:%S')
                            buy_price = place_order("buy", "call", atm, QTY)
                            logger.info(f"Call entry at {buy_price} for strike {atm}")
                            print(f"CE Entry executed at {buy_price}")
                            initiate_ws(atm,'call')
                            data_key=str(atm)+'_Call'
                            order = 1
                            sl = 0
                            ce_entry_data = [today, entry_time, atm, f"call_{EXPIRY}", buy_price]
                        else:
                            print(f"No CE position taken - Conditions not met")
                        if rsi_pe_condition and macd_pe_condition:
                            nifty = nifty_spot()
                            atm = round(nifty / 50) * 50
                            entry_time = now.strftime('%H:%M:%S')
                            buy_pe_price = place_order("buy", "put", atm, QTY)
                            logger.info(f"Put entry at {buy_pe_price} for strike {atm}")
                            print(f"PE Entry executed at {buy_pe_price}")
                            initiate_ws(atm,'put')
                            data_key_pe=str(atm)+'_Put'
                            order = -1
                            sl = 0
                            pe_entry_data = [today, entry_time, atm, f"put_{EXPIRY}", buy_pe_price]
                        else:
                            print("No PE position taken - Conditions not met")
                    except Exception as e:
                        logger.error(f"Error in PE entry logic: {str(e)}")
                        print(f"Error in PE entry: {str(e)}")
                
                # CE Exit Logic
                if order == 1:
                    try:
                        time.sleep(20)
                        olhc = fetch_historical( yesterday, today)
                        # ce_option.ta.rsi(close='close', length=14, append=True)
                        # supertrend = ta.supertrend(ce_option['high'], ce_option['low'], 
                        #                         ce_option['close'], length=10, multiplier=2)
                        # ce_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                        # last_row = ce_option.iloc[-1]
                        last_row = olhc.iloc[-1]
                        second_last = olhc.iloc[-2]
                        third_last = olhc.iloc[-3]
                        macd_ce_exit=(last_row['MACD'] < second_last['MACD'] and second_last['MACD'] < third_last['MACD'])
                        
                    
                        if data_key in tick_data:
                            leg1_cmp=tick_data[data_key]
                            print(f"CE Current Market Price: {leg1_cmp}")
                            
                            profit_condition = (leg1_cmp - buy_price) >= 10
                            if profit_condition:
                                sl = 1
                                print("CE SL triggered (profit >= 10)")
                            sl_hit = sl == 1 and leg1_cmp <= buy_price
                            if sl_hit:
                                sl = 2
                                logger.info("SL Hit for CE")
                                print("CE SL Hit")
                            
                            # rsi_exit = last_row['RSI_14'] < 70
                            # supertrend_exit = last_row['supertrend'] != 1
                            time_exit = current_time >= datetime.strptime("15:19", "%H:%M").time()
                            print(f"CE Exit Conditions - RSI: {last_row['RSI_14']} (macd: {macd_ce_exit}), "
                                f"Time Exit: {time_exit}, SL: {sl == 2}")
                            
                            if macd_ce_exit or time_exit or sl == 2:
                                sell_price = place_order("sell", "call", atm, QTY)
                                exit_time = now.strftime('%H:%M:%S')
                                pnl = round(sell_price - buy_price, 2)
                                logger.info(f"Call exit, PnL: {pnl}")
                                print(f"CE Exit executed at {sell_price}, PnL: {pnl}")
                                write_to_csv(ce_entry_data + [exit_time, sell_price, pnl, QTY])
                                deactivate_ws(atm,'call')
                                order = 0
                            else:
                                print("No CE exit - Conditions not met")
                        else:
                            print(f"Strike:{data_key} not in tick data:{tick_data} ")
                    except Exception as e:
                        logger.error(f"Error in CE exit logic: {str(e)}")
                        print(f"Error in CE exit: {str(e)}")
                elif order == -1:
                    try:
                        # time.sleep(20)
                        olhc = fetch_historical( yesterday, today)
                        # ce_option.ta.rsi(close='close', length=14, append=True)
                        # supertrend = ta.supertrend(ce_option['high'], ce_option['low'], 
                        #                         ce_option['close'], length=10, multiplier=2)
                        # ce_option['supertrend'] = supertrend['SUPERTd_10_2.0']
                        # last_row = ce_option.iloc[-1]
                        last_row = olhc.iloc[-1]
                        second_last = olhc.iloc[-2]
                        third_last = olhc.iloc[-3]
                        macd_pe_exit=(last_row['MACD'] > second_last['MACD'] and second_last['MACD'] > third_last['MACD'])
                        
                    
                        if data_key_pe in tick_data:
                            leg2_cmp=tick_data[data_key_pe]
                            print(f"PE Current Market Price: {leg2_cmp}")
                            
                            profit_condition = (leg2_cmp - buy_pe_price) >= 15
                            if profit_condition:
                                sl = 1
                                print("PE SL triggered (profit >= 10)")
                            sl_hit = sl == 1 and leg2_cmp <= buy_pe_price
                            if sl_hit:
                                sl = 2
                                logger.info("SL Hit for PE")
                                print("PE SL Hit")
                            
                            # rsi_exit = last_row['RSI_14'] < 70
                            # supertrend_exit = last_row['supertrend'] != 1
                            time_exit = current_time >= datetime.strptime("15:19", "%H:%M").time()
                            print(f"PE Exit Conditions - RSI: {last_row['RSI_14']} (macd:{macd_pe_exit}), "
                                f"Time Exit: {time_exit}, SL: {sl == 2}")
                            
                            if macd_pe_exit or time_exit or sl == 2:
                                sell_price = place_order("sell", "call", atm, QTY)
                                exit_time = now.strftime('%H:%M:%S')
                                pnl = round(sell_price - buy_pe_price, 2)
                                logger.info(f"PUT exit, PnL: {pnl}")
                                print(f"PE Exit executed at {sell_price}, PnL: {pnl}")
                                write_to_csv(pe_entry_data + [exit_time, sell_price, pnl, QTY])
                                deactivate_ws(atm,'put')
                                order = 0
                            else:
                                print("No PE exit - Conditions not met")
                        else:
                            print(f"Strike:{data_key_pe} not in tick data:{tick_data} ")
                    except Exception as e:
                        logger.error(f"Error in PE exit logic: {str(e)}")
                        print(f"Error in PE exit: {str(e)}")
                
                # PE Entry Logic
                
            else:
                print("Outside trading hours")
            
            time.sleep(1)  # Prevent CPU overload
            
        except Exception as e:
            logger.error(f"Main loop error: {str(e)}")
            print(f"Main loop error: {str(e)}")
            time.sleep(60)

if __name__ == "__main__":
    main()
