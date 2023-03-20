import pandas as pd
import sys
import subprocess
import os

sys.path.append('../hapi')
from hist_config import SYMBOLS_PATH

def run_binance_stresstest(n_symbols:int=None, full_test:bool=False, channel:'trade'=None):
    test_base_tokens = ['BTC', 'ETH', 'USDT','ALGO', 'UNI', 'LINK', 'DOGE', 'BNB', 'ADA', 'SRM', 'ATOM', 'EUR']
    symbols = pd.read_excel(os.path.join(SYMBOLS_PATH, 'binance_symbols.xlsx'))
    filter_symbols = (symbols['status'] == 'TRADING') 
    if not full_test: 
        filter_symbols = filter_symbols & (symbols['baseAsset'].isin(test_base_tokens))
    symbols = list(symbols[filter_symbols]['symbol'])
    symbols = symbols[:n_symbols]
    processes = []
    for i, symbol in enumerate(symbols,0):
        proc = subprocess.Popen(['python', 'build_stream.py', 'binance', symbol, channel])
        processes.append(proc)
    
    for i, proc in enumerate(processes, 1):
        proc.wait()

if __name__ == '__main__':
    full_test = False # use with care as this leads to extreme memory load
    channel = sys.argv[1] if len(sys.argv) >1 else 'trade'
    n_symbols = int(sys.argv[1]) if len(sys.argv) >2 else None
    run_binance_stresstest(n_symbols, full_test=full_test, channel=channel)
