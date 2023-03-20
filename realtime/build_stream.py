### Streaming Reatime Data ###
import asyncio
import uvloop
import ujson as json
import sys
import pandas as pd
import os
import concurrent.futures
import multiprocessing

import config as cfg
from wss_stream_async import WssStream
from db_handler_async import RealTimePeon

sys.path.append('../hapi')
from hist_config import SYMBOLS_PATH

async def main():
    exchange = sys.argv[1] if len(sys.argv)>1 else 'binance'
    symbol = sys.argv[2] if len(sys.argv)>2 else 'btcusdt'
    channel = sys.argv[3] if len(sys.argv)>3 else 'ob'
    runtime = int(sys.argv[4]) if len(sys.argv)>4 else 60*60*24
    
    # Queue Initialisation
    q_wss_to_bq = asyncio.Queue()
    bq_to_db = asyncio.Queue()

    # Init Db Connection
    RTP = RealTimePeon(symbol=symbol, exchange=exchange, channel=channel)

    # Init WSS connection
    channel_buffer_specifics = cfg.CHANNEL_BUFFER_LIMITS[channel]
    tasks = []
    WssConnection = WssStream(runtime=runtime)    
    WssConnection.init_stream_setup(symbol=symbol, exchange=exchange, channel=channel)
    q_db = asyncio.create_task(RTP.queue_batches(*channel_buffer_specifics[0], q_wss_to_bq, bq_to_db))
    p_db = asyncio.create_task(RTP.stream_to_database(*channel_buffer_specifics[1], bq_to_db))
    wss = asyncio.create_task(WssConnection.start_stream(symbol, q_wss_to_bq))
    tasks.extend([q_db, p_db, wss])

    await asyncio.gather(*tasks)

    print(f"### {f'Process finished from {exchange}':^50} ###")
    if not q_wss_to_bq.empty():
        print(f"### {'Queue is not empty!':^50} ###")

   
async def gather_coroutines(runtime:int, symbols:list, exchange:str, channel:str) -> None:
    channel_buffer_specifics = cfg.CHANNEL_BUFFER_LIMITS[channel]
    tasks = []
    for symbol in symbols:
        # Queue Initialisation
        q_wss_to_bq = asyncio.Queue()
        bq_to_db = asyncio.Queue()

        # Init Db Connection
        RTP = RealTimePeon(symbol=symbol, exchange=exchange, channel=channel)
        RTP.init_table_db()

        # Init WSS connection
        WssConnection = WssStream(runtime=runtime)   
        WssConnection.init_stream_setup(symbol=symbol, exchange=exchange, channel=channel)
        q_db = asyncio.create_task(RTP.queue_batches(*channel_buffer_specifics[0], q_wss_to_bq, 
            bq_to_db))
        p_db = asyncio.create_task(RTP.stream_to_database(*channel_buffer_specifics[1], 
            bq_to_db))
        wss = asyncio.create_task(WssConnection.start_stream(symbol, q_wss_to_bq))
        tasks.extend([q_db, p_db, wss])

    await asyncio.gather(*tasks)

    print(f"### {f'Process finished from {exchange}':^50} ###")
    if not q_wss_to_bq.empty():
        print(f"### {'Queue is not empty!':^50} ###")

async def run_kraken_multistream(n_symbols:int=None, full_test:bool=False, 
        channel:str='ob', exchange:str='kraken'):
    runtime = 60 * 60 * 24
    test_base_tokens = ['XXBT', 'ETH', 'ZUSD', 'UNI', 'LINK', 'DOGE', 'SHIB', 'ZEUR']

    with open(os.path.join(SYMBOLS_PATH, 'kraken.json')) as json_file:
        symbols = pd.json_normalize([x for y,x in json.load(json_file)['result'].items()]) 
     
    if not full_test: 
        filter_symbols = (symbols['base'].isin(test_base_tokens))
    symbols = list(symbols[filter_symbols]['wsname'])
    symbols = symbols[:n_symbols]
    await gather_coroutines(runtime, symbols, exchange, channel)

async def run_binance_multistream(n_symbols:int=None, full_test:bool=False, 
        channel:str='ob', exchange:str='binance'):
    runtime = 60*60*24
    test_base_tokens = ['BTC', 'ETH', 'USDT','ALGO', 'UNI', 'LINK', 'DOGE', 'SHIB', 'SPELL', 'BNB']
    
    symbols = pd.read_excel(os.path.join(SYMBOLS_PATH, 'binance_symbols.xlsx'))
    filter_symbols = (symbols['status'] == 'TRADING') 
    if not full_test: 
        filter_symbols = filter_symbols & (symbols['baseAsset'].isin(test_base_tokens))
    symbols = list(symbols[filter_symbols]['symbol'])
    symbols = symbols[:n_symbols]
    await gather_coroutines(runtime, symbols, exchange, channel)

async def run_binance_multistream_mp(n_symbols:int=None, full_test:bool=False, 
        channel:str='ob', exchange:str='binance'):
    runtime = 60*60*24
    test_base_tokens = ['BTC', 'ETH', 'USDT','ALGO', 'UNI', 'LINK', 'DOGE', 'SHIB', 'SPELL', 
            'BNB', 'ADA', 'SRM', 'ATOM', 'EUR']
    
    symbols = pd.read_excel(os.path.join(SYMBOLS_PATH, 'binance_symbols.xlsx'))
    filter_symbols = (symbols['status'] == 'TRADING') 
    if not full_test: 
        filter_symbols = filter_symbols & (symbols['baseAsset'].isin(test_base_tokens))
    symbols = list(symbols[filter_symbols]['symbol'])
    symbols = symbols[:n_symbols]
    task_list = []
    
    channel_buffer_specifics = cfg.CHANNEL_BUFFER_LIMITS[channel]
    """ Generate a process pool to delegate cpu_bound tasks from the event loop to idle 
        processes to avoid blocking the main (event-loop) thread """ 
    with concurrent.futures.ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()-1) as pool:
        for symbol in symbols:
            WStream = WssStream(verbose=True, queueing=False, runtime=runtime)
            tasks = WStream.setup_start_alt(symbol, channel, exchange, pool) 
            task_list.extend(tasks)
        await asyncio.gather(*task_list)

    print(f"### {f'Process finished from {exchange}':^50} ###")

if __name__ == '__main__':
    if sys.platform[0].lower() != 'w':
        uvloop.install()
    if len(sys.argv)>0:
        channel = sys.argv[1] if len(sys.argv) > 1 else 'bbo'
    #asyncio.run(run_kraken_multistream(n_symbols=50, channel=channel))
    #asyncio.run(main())
    #asyncio.run(run_binance_multistream_mp(n_symbols=350, channel=channel, full_test=True))
    asyncio.run(run_binance_multistream(n_symbols=100, channel=channel, full_test=False))

