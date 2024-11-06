import requests
import logging
import time
import json
import pandas as pd

def retry_api_call(func, retries=5, delay=5, backoff=2):
    """Retry API calls with exponential backoff."""
    attempt = 0
    while attempt < retries:
        try:
            return func()
        except Exception as e:
            logging.error(f"Error during API call: {e}, Retrying in {delay} seconds...")
            attempt += 1
            time.sleep(delay)
            delay *= backoff
    raise Exception(f"API call failed after {retries} attempts.")

def get_nifty_future_instrument_id(access_token, expiry_date):
    url = f"https://ttblaze.iifl.com/apimarketdata/instruments/instrument/futureSymbol?exchangeSegment=2&series=FUTIDX&symbol=NIFTY&expiryDate={expiry_date}"
    headers = {
        "Authorization": f"{access_token}",
    }
    response = retry_api_call(lambda: requests.get(url, headers=headers))
    if response.status_code == 200:
        id = response.json()       
        # Get the ExchangeInstrumentID
        exchange_instrument_id = id['result'][0]['ExchangeInstrumentID']
        return exchange_instrument_id
    else:
        logging.error("ExchangeInstrumentID not found in the response.")
        return None

def get_nifty_future_ohlc_with_retry(access_token, exchange_instrument_id, start_time, end_time, compression_value=60):
    url = f"https://ttblaze.iifl.com/apimarketdata/instruments/ohlc?exchangeSegment=2&exchangeInstrumentID={exchange_instrument_id}&startTime={start_time}&endTime={end_time}&compressionValue={compression_value}"
    headers = {
        "Authorization": f"{access_token}", 
    }
    
    ohlc_response =  retry_api_call(lambda: requests.get(url, headers=headers))
    if ohlc_response.status_code == 200:
        ohlc_data = ohlc_response.json()
    else:
        print("Error fetching OHLC data:", ohlc_response.text)
    return ohlc_data

def get_nifty_option_instrument(access_token, expiry_date, strike_price, option_type):
    url = f"https://ttblaze.iifl.com/apimarketdata/instruments/instrument/optionSymbol?exchangeSegment=2&series=OPTIDX&symbol=NIFTY&expiryDate={expiry_date}&strikePrice={strike_price}&optionType={option_type}"
    headers = {
        "Authorization": f"{access_token}",
    }
    response = retry_api_call(lambda: requests.get(url, headers=headers))
    if response.status_code == 200:
        option_instrument = response.json()
        option_instrument_id = option_instrument['result'][0]['ExchangeInstrumentID']
    return option_instrument_id

def get_nifty_option_ohlc_with_retry(access_token, exchange_instrument_id, start_time, end_time, compression_value=60):
    url = f"https://ttblaze.iifl.com/apimarketdata/instruments/ohlc?exchangeSegment=2&exchangeInstrumentID={exchange_instrument_id}&startTime={start_time}&endTime={end_time}&compressionValue={compression_value}"
    headers = {
        "Authorization": f"{access_token}", 
    }
    ohlc_data_response = retry_api_call(lambda: requests.get(url, headers=headers))
    if ohlc_data_response.status_code == 200:
        ohlc_data = ohlc_data_response.json()
    return ohlc_data

def get_ltp_with_retry(access_token, exchange_segment, instrument_id):
    url = "https://ttblaze.iifl.com/apimarketdata/instruments/quotes"
    headers = {
        'Authorization': f'{access_token}',
        'Content-Type': 'application/json'
    }
    payload = {
        "instruments": [
            {
                "exchangeSegment": exchange_segment,  # For NSECM (NSE Cash Market)
                "exchangeInstrumentID": instrument_id  #26000 for nifty cash
            }
        ],
        "xtsMessageCode": 1512,  # For LTP (Last Traded Price)
        "publishFormat": "JSON"
    }    
    response = retry_api_call(lambda: requests.post(url, headers=headers, json=payload))
    
    if response.status_code == 200:
        quote_data = response.json()
        last_traded_price = json.loads(quote_data['result']['listQuotes'][0]).get('LastTradedPrice', 'N/A')  
        return last_traded_price
    else:
        return f"Error: {response.status_code} {response.text}"

def get_order_detail(interactive_access_token, app_order_id):
    url = f"https://ttblaze.iifl.com/interactive/orders?appOrderID={app_order_id}"
    headers = {
        'Authorization': f'{interactive_access_token}'
    }                                                                                                                                                                            
    response = requests.get(url, headers=headers)
    
    if response.status_code == 200:
        order_details = response.json()
        # Extract Last Traded Price (LTP) if available in the response
        for order in order_details['result']:
            ltp = order.get('OrderAverageTradedPrice', 'N/A')  # LTP can be in 'OrderAverageTradedPrice'
            #print(f"Last Traded Price (LTP): {ltp}")
        return float(ltp)
    else:
        print(f"Error: {response.status_code}", response.text)
        return None

def place_options_order(interactive_access_token, exchange_instrument_id, quantity, order_side):
    url = "https://ttblaze.iifl.com/interactive/orders"
    headers = {
        'Authorization': f'{interactive_access_token}',
        'Content-Type': 'application/json'
    }
    
    payload = {
        "exchangeSegment": "NSEFO",  
        "exchangeInstrumentID": exchange_instrument_id,
        "productType": "NRML", 
        "orderType": "MARKET",  
        "orderSide": order_side,     
        "timeInForce": "DAY",
        "disclosedQuantity": 0,
        "orderQuantity": quantity,
        "limitPrice": 0,
        "stopPrice": 0,
        "orderUniqueIdentifier": "order123"
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        return response.json()['result']['AppOrderID']
    else:
        print(f"Error: {response.status_code}", response.text)