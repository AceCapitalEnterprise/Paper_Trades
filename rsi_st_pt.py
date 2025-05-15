import numpy as np
import pandas as pd
import pandas_ta as ta
from datetime import date, datetime, timedelta, time as t
import csv
import time
import logging
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
import concurrent.futures
from breeze_connect import BreezeConnect
import urllib
from scipy.stats import norm

# Initialize BreezeConnect
breeze = BreezeConnect(api_key="64mV026553514z5565%7S258@^4l3753")
import urllib
breeze.generate_session(api_secret="20286x551)P23443543722J0t28s90D3",
                        session_token="51349293")


# Configure logging
logging.basicConfig(
    filename='trading_bot.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Constants
TIME_1 = t(9, 15)
TIME_2 = t(15, 20)
EXPIRY = '2025-04-30'
# QTY = 75
MAX_RETRIES = 10
RATE_LIMIT_DELAY = 3
RISK_FREE_RATE = 0.07  # 7% annual risk-free rate
W_DELTA = 0.5  # Weight for Delta
W_THETA = 0.3  # Weight for Theta
W_VEGA = 0.2   # Weight for Vega

# Global state
order = 0
order2 = 0
sl = 0
sl2 = 0
buy_price = 0
buy_pe_price = 0

# Black-Scholes Greeks Calculation
def black_scholes_greeks(spot, strike, time_to_expiry, volatility, risk_free_rate, option_type="call"):
    """Calculate Delta, Theta, and Vega using Black-Scholes model."""
    if time_to_expiry <= 0 or volatility <= 0:
        return 0, 0, 0
    
    d1 = (np.log(spot / strike) + (risk_free_rate + 0.5 * volatility**2) * time_to_expiry) / (volatility * np.sqrt(time_to_expiry))
    d2 = d1 - volatility * np.sqrt(time_to_expiry)
    
    if option_type == "call":
        delta = norm.cdf(d1)
        theta = (-spot * norm.pdf(d1) * volatility / (2 * np.sqrt(time_to_expiry)) - 
                 risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2)) / 365
    else:  # put
        delta = norm.cdf(d1) - 1
        theta = (-spot * norm.pdf(d1) * volatility / (2 * np.sqrt(time_to_expiry)) + 
                 risk_free_rate * strike * np.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2)) / 365
    
    vega = spot * norm.pdf(d1) * np.sqrt(time_to_expiry) / 100  # Vega per 1% IV change
    
    return delta, theta, vega
class TradingBot:
    def __init__(self, breeze):
        self.breeze = breeze
        self.today = datetime.now().strftime('%Y-%m-%d')
        self.yesterday = (datetime.now() - timedelta(days=5)).strftime('%Y-%m-%d')
        self.expiry_date = datetime.strptime(EXPIRY, '%Y-%m-%d')
        self.expiry1='27-Mar-2025'
        self.spot_price=None
        self.breeze.ws_connect()
        self.breeze.on_ticks=self.on_ticks

    def on_ticks(self,ticks):
        # global one_tick
        # print("-------------------------------------------------------------")
        self.spot_price=ticks['last']

        # print(ticks)


    def initiate_ws(self, atm,right=''):
        if right=='call':    
            # print("hello")
            leg=self.breeze.subscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    # expiry_date=f'{expiry}T06:00:00.000Z',
                                    expiry_date=self.expiry1,
                                    right="call",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
        # print(leg)
        elif right=='put':
            leg2=self.breeze.subscribe_feeds(exchange_code="NFO",
                                    stock_code="NIFTY",
                                    product_type="options",
                                    expiry_date=self.expiry1,
                                    right="put",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
            # print(leg2)
        elif right=='others':
            leg3=breeze.subscribe_feeds(exchange_code="NSE",
                                    stock_code="NIFTY",
                                    product_type="CASH",
                                    expiry_date=self.expiry1,
                                    right="others",
                                    strike_price=str(atm),
                                    get_exchange_quotes=True,
                                    get_market_depth=False)
            print(leg3)
        

    def deactivate_ws(self, atm,right=''):
        if right=='call':    
            self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right="call",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)
        elif right=='put':
            self.breeze.unsubscribe_feeds(exchange_code="NFO",
                                stock_code="NIFTY",
                                product_type="options",
                                expiry_date=self.expiry1,
                                right="put",
                                strike_price=str(atm),
                                get_exchange_quotes=True,
                                get_market_depth=False)

    @retry(stop=stop_after_attempt(MAX_RETRIES), 
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type(Exception))
    def nifty_spot(self):
        try:
            # nifty = self.breeze.get_quotes(stock_code="NIFTY",
            #                                exchange_code="NSE",
            #                                expiry_date=f"{self.today}T06:00:00.000Z",
            #                                product_type="cash",
            #                                right="others",
            #                                strike_price="0"
            #                               )
            # time.sleep(RATE_LIMIT_DELAY)
            # if nifty.get('Success'):
            #     return float(pd.DataFrame(nifty['Success'])['ltp'][0])
            if self.spot_price is not None:
                return float(self.spot_price)
            raise ValueError("No success response from API")
        except Exception as e:
            logging.error(f"Error fetching Nifty spot: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(MAX_RETRIES),
           wait=wait_exponential(multiplier=1, min=4, max=10),
           retry=retry_if_exception_type(Exception))
    def option_historical(self, ce_or_pe, strike):
        try:
            option_data = self.breeze.get_historical_data_v2(interval="1minute",
                                                              from_date=f"{self.yesterday}T07:00:00.000Z",
                                                              to_date=f"{self.today}T17:00:00.000Z",
                                                              stock_code="NIFTY",
                                                              exchange_code="NFO",
                                                              product_type="options",
                                                              expiry_date=f"{EXPIRY}T07:00:00.000Z",
                                                              right=ce_or_pe,
                                                              strike_price=strike
                                                            )
            time.sleep(RATE_LIMIT_DELAY)
            if option_data.get('Success'):
                return pd.DataFrame(option_data['Success'])
            raise ValueError("No success response from API")
        except Exception as e:
            logging.error(f"Error fetching historical data {ce_or_pe} {strike}: {str(e)}")
            raise

    def calculate_indicators(self, df):
        df.ta.rsi(close='close', length=14, append=True)
        supertrend = ta.supertrend(df['high'], df['low'], df['close'], length=10, multiplier=2)
        df['supertrend'] = supertrend['SUPERTd_10_2.0']
        df['volume_avg'] = df['volume'].rolling(window=5).mean()
        df['volume_check'] = (df['volume'] > 1.5 * df['volume_avg']).astype(int)
        df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)  # Still used for volatility approximation
        return df

    def place_order(self, action, right, strike, price=""):
        for _ in range(MAX_RETRIES):
            try:
                # order_detail = self.breeze.place_order(
                #     stock_code="NIFTY",
                #     exchange_code="NFO",
                #     product="options",
                #     action=action,
                #     order_type="market",
                #     stoploss="",
                #     quantity=QTY,
                #     price=price,
                #     validity="day",
                #     disclosed_quantity="0",
                #     expiry_date=f'{EXPIRY}T06:00:00.000Z',
                #     right=right,
                #     strike_price=strike
                # )
                # if order_detail.get('Success'):
                #     order_id = order_detail['Success']['order_id']
                #     trade_detail = self.breeze.get_trade_detail(exchange_code="NFO", order_id=order_id)
                #     if trade_detail.get('Success'):
                #         return float(pd.DataFrame(trade_detail['Success'])['execution_price'][0])
                leg=self.breeze.get_option_chain_quotes(stock_code="NIFTY",
                                               exchange_code="NFO",
                                               product_type="options",
                                               expiry_date=f'{EXPIRY}T06:00:00.000Z',
                                               right=right,
                                               strike_price=strike)
                time.sleep(RATE_LIMIT_DELAY)
                leg_df = leg['Success']
                leg_df = pd.DataFrame(leg_df)
                ltp_value = float(leg_df['ltp'][0])
                return ltp_value
            except Exception as e:
                logging.warning(f"Order placement attempt failed: {str(e)}")
                time.sleep(RATE_LIMIT_DELAY)
        raise Exception("Failed to place order after retries")

    def get_current_price(self, right, strike):
        try:
            quotes = self.breeze.get_option_chain_quotes(
                stock_code="NIFTY",
                exchange_code="NFO",
                product_type="options",
                expiry_date=f'{EXPIRY}T06:00:00.000Z',
                right=right,
                strike_price=strike
            )
            time.sleep(RATE_LIMIT_DELAY)
            if quotes.get('Success'):
                return float(pd.DataFrame(quotes['Success'])['ltp'][0])
            return None
        except Exception as e:
            logging.error(f"Error getting current price: {str(e)}")
            return None

    def exit_position(self, right, strike, buy_price, reason):
        sell_price = self.place_order("sell", right, strike)
        pnl = round(sell_price - buy_price, 2)
        logging.info(f"{right} exit, reason: {reason}, PnL: {pnl}")
        
        csv_file = "rsi_supertrend_RPS_new.csv"
        entry_time = datetime.now().strftime('%H:%M:%S')  # Should store at entry
        try:
            with open(csv_file, 'x', newline='') as file:
                writer = csv.writer(file)
                writer.writerow(['Date', 'Entry Time', 'Strike', 'CE or PE', 'Entry premium', 
                               'Exit Time', 'Exit premium', 'PnL'])
        except FileExistsError:
            with open(csv_file, 'a', newline='') as file:
                writer = csv.writer(file)
                writer.writerow([self.today, entry_time, strike, f'{right}_{EXPIRY}', 
                               buy_price, datetime.now().strftime('%H:%M:%S'), 
                               sell_price, pnl])
        return sell_price

    def calculate_greeks_sl(self, current_price, nifty, strike, time_to_expiry, volatility, option_type, atr):
        delta, theta, vega = black_scholes_greeks(nifty, strike, time_to_expiry, volatility, RISK_FREE_RATE, option_type)
        
        # Time factor: remaining minutes / total trading minutes (365 minutes from 9:15 to 15:20)
        now = datetime.now()
        total_minutes = 365
        minutes_left = (TIME_2.hour * 60 + TIME_2.minute) - (now.hour * 60 + now.minute)
        time_factor = minutes_left / total_minutes if minutes_left > 0 else 0.01
        
        # Use ATR as a proxy for SpotMove and VolFactor
        spot_move = atr
        vol_factor = atr / nifty
        
        # Greeks-based SL offset
        sl_offset = (W_DELTA * abs(delta) * spot_move + 
                    W_THETA * abs(theta) * time_factor + 
                    W_VEGA * vega * vol_factor)
        
        return current_price - sl_offset, delta, theta, vega

    def handle_call_options(self):
        global order, sl, buy_price
        try:
            nifty = self.nifty_spot()
            ce_otm = round(nifty / 50) * 50 + 100
            ce_option = self.calculate_indicators(self.option_historical("call", ce_otm))
            last_row = ce_option.iloc[-1]
            volatility = last_row['atr'] / nifty  # Approximate IV from ATR
            time_to_expiry = (self.expiry_date - datetime.now()).days / 365.0

            if order == 0 and last_row['RSI_14'] > 70 and last_row['supertrend'] == 1 and last_row['volume_check'] == 1:
                buy_price = self.place_order("buy", "call", ce_otm)
                sl, delta, theta, vega = self.calculate_greeks_sl(buy_price, nifty, ce_otm, time_to_expiry, volatility, "call", last_row['atr'])
                logging.info(f"Call entry at {buy_price}, Initial SL: {sl:.2f}, Delta: {delta:.3f}, Theta: {theta:.3f}, Vega: {vega:.3f}")
                print('hello')
                order = 1
                return order, sl

            else:
                print('NO Condition')
            
            if order == 1:
                current_price = self.get_current_price("call", ce_otm)
                if current_price:
                    sl_new, delta, theta, vega = self.calculate_greeks_sl(current_price, nifty, ce_otm, time_to_expiry, volatility, "call", last_row['atr'])
                    sl = max(sl, sl_new)  # Trail up only
                    logging.info(f"Call current: {current_price}, Trailing SL: {sl:.2f}, Delta: {delta:.3f}, Theta: {theta:.3f}, Vega: {vega:.3f}")
                    
                    if current_price <= sl:
                        self.exit_position("call", ce_otm, buy_price, "Greeks SL Hit")
                        order = 0
                    elif last_row['RSI_14'] < 70 or last_row['supertrend'] != 1 or t(datetime.now().hour, datetime.now().minute) == t(15,19):
                        self.exit_position("call", ce_otm, buy_price, "Condition Exit")
                        order = 0
            return order, sl

        except Exception as e:
            logging.error(f"Call options error: {str(e)}")
            return order, sl

    def handle_put_options(self):
        global order2, sl2, buy_pe_price
        try:
            nifty = self.nifty_spot()
            pe_otm = round(nifty / 50) * 50 - 100
            pe_option = self.calculate_indicators(self.option_historical("put", pe_otm))
            last_row = pe_option.iloc[-1]
            volatility = last_row['atr'] / nifty  # Approximate IV from ATR
            time_to_expiry = (self.expiry_date - datetime.now()).days / 365.0

            if order2 == 0 and last_row['RSI_14'] > 70 and last_row['supertrend'] == 1 and last_row['volume_check'] == 1:
                buy_pe_price = self.place_order("buy", "put", pe_otm)
                sl2, delta, theta, vega = self.calculate_greeks_sl(buy_pe_price, nifty, pe_otm, time_to_expiry, volatility, "put", last_row['atr'])
                logging.info(f"Put entry at {buy_pe_price}, Initial SL: {sl2:.2f}, Delta: {delta:.3f}, Theta: {theta:.3f}, Vega: {vega:.3f}")
                print('hello put')
                order2 = 1
                return order2, sl2
            else:
                print('no condition put')
            if order2 == 1:
                current_price = self.get_current_price("put", pe_otm)
                if current_price:
                    sl_new, delta, theta, vega = self.calculate_greeks_sl(current_price, nifty, pe_otm, time_to_expiry, volatility, "put", last_row['atr'])
                    sl2 = max(sl2, sl_new)  # Trail up only
                    logging.info(f"Put current: {current_price}, Trailing SL: {sl2:.2f}, Delta: {delta:.3f}, Theta: {theta:.3f}, Vega: {vega:.3f}")
                    
                    if current_price <= sl2:
                        self.exit_position("put", pe_otm, buy_pe_price, "Greeks SL Hit")
                        order2 = 0
                    elif last_row['RSI_14'] < 70 or last_row['supertrend'] != 1 or t(datetime.now().hour, datetime.now().minute) == t(15,19):
                        self.exit_position("put", pe_otm, buy_pe_price, "Condition Exit")
                        order2 = 0
            return order2, sl2

        except Exception as e:
            logging.error(f"Put options error: {str(e)}")
            return order2, sl2
        

        
def main():
    bot = TradingBot(breeze)
    bot.initiate_ws('0','others')
    while True:
        try:
            now = datetime.now()
            current_time = t(now.hour, now.minute)
            
            if TIME_1 < current_time < TIME_2 and now.second == 0:
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    future_ce = executor.submit(bot.handle_call_options)
                    future_pe = executor.submit(bot.handle_put_options)
                    global order, sl, order2, sl2
                    order, sl = future_ce.result()
                    order2, sl2 = future_pe.result()
                
                time.sleep(1)
                
        except Exception as e:
            logging.error(f"Main loop error: {str(e)}")
            time.sleep(5)

if __name__ == "__main__":
    main()