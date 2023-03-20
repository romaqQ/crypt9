import time
import json
import sys
import os
import uvloop
import asyncio
import concurrent.futures
import requests
import hmac
import hashlib

from wss_stream_async import WssStream

API_KEY = 'INSERT_HERE'
SECRET_KEY = 'INSERT_HERE'
HEADER = {'X-MBX-APIKEY':API_KEY}
API_BASE = 'https://api.binance.com'
file_path = '../data'
file_name = 'latency'

def hmac256(msg, secret_key) -> str:
    return hmac.new(bytes(secret_key, 'utf-8'), msg = bytes(msg, 'utf-8'), digestmod = hashlib.sha256).hexdigest()

def post_request(url):
    res = requests.post(url, headers=HEADER)
    print(res.json())
    return res.json()

def get_request(url):
    res = requests.get(url, headers=HEADER)
    return res.json()

def retrieve_listen_key():
    API_ENDPOINT = '/api/v3/userDataStream'
    url = f'{API_BASE}{API_ENDPOINT}'
    return post_request(url)['listenKey']

def retrieve_server_time():
    API_ENDPOINT = '/api/v3/time'
    url = f'{API_BASE}{API_ENDPOINT}'
    return get_request(url)

def send_market_order(symbol:str, side:str, quantity:int, test:bool=True, secret_key:str=SECRET_KEY) -> None:
    API_ENDPOINT = '/api/v3/order/test' if test else '/api/v3/order'
    url = f'{API_BASE}{API_ENDPOINT}' 
    ts = int(time.time() * 1000)
    href = f'symbol={symbol}&side={side}&type=MARKET&quantity={quantity}&timestamp={ts}'
    signature = hmac256(href, secret_key)
    url = f'{url}?{href}&signature={signature}'
    post_request(url)

def check_time_sync(): 
    with open(os.path.join(file_path, f'{file_name}.txt'), 'a') as text_file:
        for _ in range(5):
            t1 = int(time.time() * 1000)
            server_time = retrieve_server_time()['serverTime']
            t2 = int(time.time() * 1000)
            print(server_time - t1, t2-server_time)
            msg = f't1_SendApiReq: {t1} server_time_ApiResponse: {server_time} t2_ReceiveApiResp: {t2} - Diff(1: srvtime-t1) {server_time - t1} - Diff(2: t2 - server_time) {t2-server_time}\n'
            text_file.write(msg)

def schedule_mo_trades(n_trades:int, freq:int, symbol:str, side:str, base_quantity:int, test:bool=True):
    print('sleep- 15 ')
    time.sleep(15)
    with open(os.path.join('../data', 'trades.txt'), 'a') as text_file:
        for i in range(n_trades):
            send_market_order(symbol=symbol,side=side,quantity=base_quantity,test=test)
            print(f'{"*":^15} Trade Sent {"*":^15}')
            text_file.write(f'{i}-{int(time.time()*1000)} - quant: {base_quantity} \n')
            time.sleep(freq)
          
async def binance_stream(symbol:str, channel:str='ob', exchange:str='binance'):
    runtime = 60*60
    
    file_path = '../data'
    file_name = f'{channel}_{symbol.replace("-", "")}_{int(time.time())}'
    task_list = []

    with concurrent.futures.ProcessPoolExecutor(max_workers=2) as pool:
        Feed = WssStream(verbose=True, runtime=runtime, timing=True)
        if channel == 'user':
            listen_key = retrieve_listen_key()
            Feed.set_listen_key(listen_key)
        on_message = Feed.write_to_file
        callback_args = [file_path, file_name]
        tasks = Feed.setup_start_alt(symbol, channel, exchange, pool, on_message=on_message, callback_args=callback_args) 
        task_list.extend(tasks)
        await asyncio.gather(*task_list)
    print(f"### {f'Process finished from {exchange}':^50} ###")

def run_stream(symbol:str='BTCUSDT', channel:str='ob', exchange:str='binance')->None:
    if sys.platform[0].lower() != 'w':
        uvloop.install()
    asyncio.run(binance_stream(symbol=symbol, channel=channel))

if __name__ == '__main__':
    if sys.platform[0].lower() != 'w':
        uvloop.install()
    if len(sys.argv)>0:
        channel = sys.argv[1] if len(sys.argv) > 1 else 'trade'
    check_time_sync()
    run_stream()

