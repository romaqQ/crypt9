import sys
import os
from multiprocessing import Process
from concurrent.futures import ProcessPoolExecutor as Pool
import binance_test as bt

def schedule_processes():
    channels = ['trade', 'bbo', 'ob']
    processes = []
    args = ['BTCUSDT', '']
    for channel in channels:
        args = ('BTCUSDT', channel, 'binance')
        process = Process(target=bt.run_stream, args=args)
        processes.append(process)
    
    for process in processes:
        #process.daemon = True # does not work since chilprocess invokes daemon process on concurrent.futures.ProcessPoolExecutor
        process.start()
        process.join()

def schedule_cf_pool():
    channels = ['user', 'trade', 'bbo', 'ob']
    processes = []
    with Pool(max_workers=len(channels)) as pool:
        print(*[('BTCUSDT', channel, 'binance') for channel in channels])
        for channel in channels:
            pool.submit(bt.run_stream, 'BTCUSDT', channel, 'binance')

def send_trades():
    bt.schedule_mo_trades(n_trades=5, freq=5, symbol='BTCUSDT', side='SELL', 
            base_quantity=0.0003, test=False)

if __name__ == '__main__':
    #schedule_processes()
    #schedule_cf_pool()
    send_trades()
