import time
import sys
import os
import asyncio
from asyncio import Queue
import uvloop # is not supported for windows
import concurrent.futures

import ujson as json
import websockets # pip install websockets
import config as cfg
import exchange as ex

from typing import Optional, Union
from collections.abc import Callable

import logging

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(thread)s %(funcName)s %(message)s")

### WSS Connection ###   
class WssStream:
    
    http_proxy_host = cfg.config['http_proxy_host']
    http_proxy_port = cfg.config['http_proxy_port']

    def __init__(self, runtime:int = 60, verbose:bool=False, timing:bool=False):
        self.runtime = runtime
        self.verbose = verbose
        self.listen_key = None
        self.winos = True if sys.platform[0].lower() == 'w' else False  
        self.start_stream = self.start_stream if not timing else self.start_stream_timing
    
    def set_listen_key(self, listen_key):
        self.listen_key = listen_key

    def init_stream_setup(self, symbol:Optional[Union[str, list]], exchange:str = 'binance', channel:str ='trade') -> None:
        self.exchange = exchange
        self.channel = channel
        self.symbol = symbol

    def prep_singlestream_exchange_setup(self, symbol:str) -> None:
        self.exchange_config = ex.retrieve_singlestream_exchange_config(
                symbol=symbol, exchange=self.exchange, channel=self.channel)
        self.url = self.exchange_config['url'].format(self.listen_key) if self.listen_key else self.exchange_config['url']
        self.sub = self.exchange_config['sub']
        if self.channel == 'user':
            if not self.listen_key:
                sys.exit('Please provide the listen_key for the respective user channel through the set_listen_key method')
            self.sub['params'] = [self.listen_key]
        self.unsub = self.exchange_config['unsub']
        self.succ_sub = self.exchange_config['succ_sub']
        self.succ_unsub = self.exchange_config['succ_unsub']

    def init_queue(self) -> None:
        self.queue = Queue()

    async def subscribe(self, ws:object) -> None:
        await ws.send(json.dumps(self.sub))
        start = time.time()
        while True:
            data = await ws.recv()
            if self.succ_sub in data:
                print(f'Subscription to {self.exchange} - {self.channel} - {self.symbol} successful')
                break
            if time.time() - start >10:
                print(f'Failed to subscribe to {self.exchange} - {self.channel} - {self.symbol}.')
                break

    async def unsubscribe(self, ws:object) -> None:
        start = time.time()
        await ws.send(json.dumps(self.unsub))
        while True:
            data = await ws.recv()
            if self.succ_unsub in data:
                print(f'Unsub from {self.exchange} - {self.channel} - symbol: {self.symbol} successful.')
                break
            if time.time() - start > 10:
                print('Failed to unsub from {self.exchange} - channel:  {self.channel} - symbol: {self.symbol}.')
                break
    
    async def start_stream(self, symbol:str=None, queue:Queue=None) -> None:
        self.prep_singlestream_exchange_setup(symbol=symbol)
        async with websockets.connect(self.url) as ws:
            await self.subscribe(ws)
            start_time = time.time()
            while True:
                if time.time() - self.runtime > start_time:
                    break
                data = await ws.recv()
                logging.info(f'data - {self.channel} - received')
                await queue.put(data)
            await self.unsubscribe(ws)
        logging.info('done')
        await queue.put(None) 
    
    async def start_stream_timing(self, symbol:str=None, queue:Queue=None) -> None:
        self.prep_singlestream_exchange_setup(symbol=symbol)
        async with websockets.connect(self.url) as ws:
            await self.subscribe(ws)
            start_time = time.time()
            while True:
                if time.time() - self.runtime > start_time:
                    break
                data = await ws.recv()
                t = time.time()
                logging.info(f'data - {self.channel} - received')
                await queue.put([t, data])
            await self.unsubscribe(ws)
        logging.info('done')
        await queue.put(None)

    async def manage_callback(self, loop:asyncio.AbstractEventLoop, pool:concurrent.futures.ProcessPoolExecutor, on_message:Callable=None, queue:Queue=None, callback_args:list=[])->None:
        """ Sample coroutine how to schedule cpu_bound event callbacks and process them in a non-blocking way"""
        records = []
        task_list = []
        callback = self.cpu_bound_method if not on_message else on_message
        while True:
            data_incr = await queue.get()
            if data_incr:
                records.append(data_incr)
                if queue.empty():
                    # attach your callbacks here: //
                    #self.test_function_latency(time.time_ns())
                    #task_t = loop.run_in_executor(pool, self.test_process_pipe_latency, time.time_ns())
                    #task_list.append(task_t)
                    task = loop.run_in_executor(pool, callback, records, *callback_args)
                    task_list.append(task)
                    await asyncio.wait(task_list, return_when=asyncio.FIRST_COMPLETED)
                    records = []
            else:
                break
    @staticmethod
    def test_function_latency(t) -> None:
        t1 = time.time_ns()
        with open(os.path.join('../data', f'function_callback_latency.txt'), 'a') as text_file:
            text_file.write(''.join(f'{t1 - t}\n'))

 
    @staticmethod
    def test_process_pipe_latency(t) -> None:
        t1 = time.time_ns()
        with open(os.path.join('../data', f'process_callback_latency.txt'), 'a') as text_file:
            text_file.write(''.join(f'{t1 - t}\n'))

    @staticmethod
    def write_to_file(batch:list, *args) -> None:
        file_path = args[0]
        file_name = args[1]
        
        def turn_to_string(batch:list) -> list:
            parsed_list = []
            for t, msg in batch:
                parsed_list.append(f'{t} - {msg} \n')
            return parsed_list
        parsed_list = turn_to_string(batch)
        
        with open(os.path.join(file_path, f'{file_name}.txt'), 'a') as text_file:
            text_file.write(''.join(parsed_list))

    @staticmethod 
    def cpu_bound_method(batch:list = None, *args)->None:
        """Emulating cpu_bound task that takes 100ms to run """
        time.sleep(.5)
        logging.info(f'running_cpu_bound_method, processed batch length: {len(batch)}')

    async def setup_start(self, symbol:str=None, channel:str=None, exchange:str=None, n_callbacks:int=1) -> None:
        """ Start single symbol stream """
        self.init_stream_setup(symbol=symbol, channel=channel, exchange=exchange)
        self.init_queue()
        with concurrent.futures.ProcessPoolExecutor(max_workers=min(n_callbacks, os.cpu_count()-1)) as pool:
            logging.info('starting_process_pool')
            loop = asyncio.get_running_loop()  
            stream = asyncio.create_task(self.start_stream(symbol=symbol, queue=self.queue))
            manage_stream = asyncio.create_task(self.manage_callback(loop, pool, self.queue))
            logging.info('collecting_coroutines')
            await asyncio.gather(stream, manage_stream)
 
    def start(self, symbol:str=None, channel:str=None, exchange:str=None) -> None:
        logging.info('starting_coroutines')
        if not self.winos:
            uvloop.install()
        asyncio.run(self.setup_start(symbol=symbol, channel=channel, exchange=exchange))
   
    def setup_start_alt(self, symbol, channel, exchange, pool, on_message:Callable=None, callback_args:list=None) -> [concurrent.futures.Future]:
        """ Convenience function of self.setup_start that returns concurrent Futures - starting single symbol stream"""
        self.init_stream_setup(symbol=symbol, channel=channel, exchange=exchange)
        self.init_queue()
        loop = asyncio.get_running_loop()  
        logging.info('collecting_coroutines')
        stream = asyncio.create_task(self.start_stream(symbol=symbol, queue=self.queue))
        manage_stream = asyncio.create_task(self.manage_callback(loop=loop, pool=pool, queue=self.queue, on_message=on_message, callback_args=callback_args))
        return [stream, manage_stream]

    def start_alt(self, symbol:str, channel:str, exchange:str, pool:concurrent.futures.ProcessPoolExecutor) -> None:
        """ Convenience function of self.start for starting multiple coroutines """
        async def collect_tasks(symbol, channel, exchange, pool):
            tasks = self.setup_start_alt(symbol, channel, exchange, pool)
            await asyncio.gather(*tasks)
        logging.info('starting_coroutines')
        if not self.winos:
            uvloop.install()
        asyncio.run(collect_tasks(symbol=symbol, channel=channel, exchange=exchange, pool=pool))


def main():
    exchange = 'binance'
    symbol = 'btcusdt'
    channel = 'user'
    runtime = 15
    WStream = WssStream(verbose=True, runtime=runtime) 
    WStream.start(symbol=symbol, exchange=exchange, channel=channel)

def main_multiprocess():
    exchange = 'binance'
    symbol = 'btcusdt'
    channel = 'trade'
    runtime = 15 
    WStream = WssStream(verbose=True, runtime=runtime) 
    with concurrent.futures.ProcessPoolExecutor(max_workers=os.cpu_count()-1) as pool:
        WStream.start_alt(symbol=symbol, exchange=exchange, channel=channel, pool=pool)

if __name__ == '__main__':
    #main()
    main_multiprocess()

