import requests
import pandas as pd
import numpy as np 
import hist_config as hcfg
import os
import sys
import time
import datetime as dt

sys.path.append('../lib')
import utils as utl

class RequestHandler:

    def __init__(self):
        self.file_exists = None

    def handle_request(self, url:str, stream:bool=True):
        self.file_exists = False
        req = requests.get(url, stream=stream)
        if req.status_code == 200:
            self.file_exists = True
        return req
    
    def _file_name(self, req):
        return req.headers['content_disposition'].split(';')[1]
    
    def save_request(self, req, save_path:str, chunk_size:int = 8192):
        with open(save_path, 'wb') as _file:
            for chunk in req.iter_content(chunk_size=chunk_size):
                _file.write(chunk) 
            

class BinanceTrooper(RequestHandler):

    base_year = int(hcfg.YEARS[0])

    def __init__(self, exchange:str = 'binance', trading_type:str ='spot', overwrite:bool=False):
        self.exchange = exchange
        self.overwrite = overwrite
        self.base_url = hcfg.HIST_DICT[self.exchange]['base_url']
        self.target_root_folder = f'../data/exchanges/binance'
        self.target_folder = ''
        utl.create_dir(self.target_root_folder)
        self.trading_type = trading_type
    
    @staticmethod
    def get_date_range(year:int, month:int, start_date:dt.date, end_date:dt.date):
        if year == start_date.year and month == start_date.month:
            start = start_date
        else:
            start = dt.date(year, month, 1) 

        if year == end_date.year and month == end_date.month:
            end = end_date
        else:
            if month < 12:
                end = dt.date(year, month+1, 1)
            else:
                end = dt.date(year+1, 1, 1)
        return pd.date_range(start, end, freq='D').day
    
    @staticmethod
    def parse_date_string(date_str:str, date_format:str='%Y-%m-%d', int_format:bool=True):
        date = dt.datetime.strptime(date_str, date_format).date()
        if int_format:
            return date.year, date.month, date.day
        else:
            return date
       
    @staticmethod
    def get_month_range(start_date:str, end_date:str):
        return pd.date_range(start_date, end_date, freq='M').month
    
    @staticmethod
    def create_file_name(symbol:str, year:int, month:int, day:int=None, time_period:str='monthly', 
            interval:str = '1m', market_data:str='trades'):
        mdtype = ''
        if market_data == 'trades':
            mdtype = 'trades-'
        elif market_data == 'aggTrades':
            mdtype = 'aggTrades-'
        elif market_data == 'klines':
            mdtype = f'{interval}-'

        if time_period == 'monthly':
            return f'{symbol.upper()}-{mdtype}{year}-{month:02d}.zip'
        else:
            return f'{symbol.upper()}-{mdtype}{year}-{month:02d}-{day:02d}.zip'
    
    @staticmethod
    def get_path(trading_type, market_data_type, time_period, symbol, interval=None):
        trading_type_path = 'data/spot'
        if trading_type != 'spot':
            trading_type_path = f'data/futures/{trading_type}'
        if market_data_type == 'klines':
            if interval is not None:
                path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/{interval}/'
        else:
            path = f'{trading_type_path}/{time_period}/{market_data_type}/{symbol.upper()}/'
        return path
    
    @staticmethod
    def get_months(year, start_date, end_date):
        if end_date.year == start_date.year:
            return list(range(start_date.month, end_date.month+1))
        else:
            if start_date.year == year:
                return list(range(start_date.month, 13))
            elif end_date.year == year:
                return list(range(1, end_date.month))
            else:
                return list(range(1,13))
    
    @staticmethod
    def set_target_folder(root_dir:str, market_data:str, time_period:str, symbol:str, interval:str=None):
        if interval:
            return os.path.join(root_dir, market_data, time_period, symbol, interval)
        return os.path.join(root_dir, market_data, time_period, symbol)
    
    def calc_total(self, years:list, months:list, symbols:list, start_date:dt.date, end_date:dt.date):
        total = 0
        for year in years:
            if not months:
                total += len(self.get_months(year=year, start_date=start_date, end_date=end_date))
            else:
                total += len(months)
        return total * len(symbols)

    def retrieve_symbols_list(self, trading_type:str=None):
        if not trading_type:
            trading_type = self.trading_type
        try:
            response = self.handle_request(hcfg.HIST_DICT[self.exchange]['symbols_url'][trading_type])
        except KeyError:
            print(f'Trading type {trading_type} not implemented for symbols')
        self.symbols_info = pd.json_normalize(response.json()['symbols'])
        symbols_file_path = os.path.join(hcfg.SYMBOLS_PATH, 'binance_symbols.xlsx')
        if not os.path.exists(symbols_file_path):
            self.symbols_info.to_excel(symbols_file_path)
        return self.symbols_info['symbol'].to_list()        
     
    def handle_start_end_date(self, year:int, start_date:str, end_date:str=None):
        if start_date:
            start_date = self.parse_date_string(start_date, int_format=False)
        else:
            year = self.base_year if not year else int(year)
            start_date = dt.date(year,1,1)
        if end_date:
            end_date = self.parse_date_string(end_date, int_format=False)
        else:
            end_date = dt.datetime.now().date() - dt.timedelta(days=1)
        return start_date, end_date
    
    def get_file(self, symbol:str, year:int, month:int, day:int=None,
            time_period = 'monthly', market_data:str='trades', interval=None):
        filename = self.create_file_name(symbol, year, month, day, time_period, interval, market_data)
         
        path = self.get_path(trading_type=self.trading_type, market_data_type=market_data, 
                time_period=time_period, symbol=symbol,interval=interval)
        file_url = f'{self.base_url}{path}{filename}'
        
        req = self.handle_request(file_url)
        file_path = os.path.join(self.target_folder, filename)
        if self.file_exists:
            if os.path.exists(file_path):
                if self.overwrite:
                    self.save_request(req, save_path=file_path)
                else:
                    print(f'{filename} already exists in {file_path} - skipping this file.')
            else:
                self.save_request(req, save_path=file_path)
        return filename, file_url

    def run_hist_file_routine(self, symbols:list, years:list=None, months:list=None, days=None, 
            time_period:str='monthly', interval=None, market_data:str='trades', 
            start_date:str = None, end_date:str=None, save_to:str=None):
        save_to = self.target_root_folder if not save_to else save_to
        cn = 0
        est_time_left = 0
        acg_cycle_time = 0
        start = time.time()
        
        # determine years for 
        if not years:
            start_date, end_date = self.handle_start_end_date(year=None, start_date=start_date, end_date=end_date)
            years = list(range(start_date.year, end_date.year +1))
        else:
            start_date, end_date = self.handle_start_end_date(year=years[0], start_date=start_date, 
                    end_date=end_date)
        total = self.calc_total(years, months, symbols, start_date, end_date) 
        for symbol in symbols:
            self.target_folder = self.set_target_folder(self.target_root_folder, 
                    market_data=market_data, time_period=time_period, symbol=symbol, interval=interval)
            utl.create_dir(self.target_folder)
            for year in years:
                months = self.get_months(year=year, start_date=start_date, end_date=end_date) if not months else months
                for month in months:
                    if time_period == 'daily':
                        days = self.get_date_range(year, month, start_date, end_date) if not days else days
             
                        for day in days:
                            fn = self.get_file(symbol=symbol, year=year, month=month, day=day,
                                    market_data=market_data,time_period=time_period, interval=interval)
                            if self.file_exists:
                                print(f'File {fn[0]} processed and saved.', end='\r')
                            else:
                                print(f'File {fn[0]} does not exist under the following file_url: {fn[1]}', end='\r')
                    else:
                        fn = self.get_file(symbol=symbol, year=year, month=month,
                                    market_data=market_data,time_period=time_period, interval=interval)
                        if self.file_exists:
                            print(f'File {fn[0]} processed and saved.', end='\r')
                        else:
                            print(f'File {fn[0]} does not exist under the following file_url: {fn[1]}', end='\r')

                    cn += 1
                    avg_cycle_time = (time.time()-start)/cn
                    est_time_left = (avg_cycle_time*(total-cn))/60
                    if total >= 25:
                        if cn % round(total/25,-1) == 0:
                            print((f'{cn/total:.2%} - Time passed: {(time.time()-start)/60:.3f} min - est.'
                                    f'time left: {est_time_left:.3f} min - avg. time per cycle:'
                                    f'{avg_cycle_time:.3f} sec.'))
        return None

    
def get_trades(symbols:list,years:list=None, months:list=None, days:list=None, 
        time_period:str='monthly', start_date:str=None, end_date:str=None):
    BT = BinanceTrooper()
    if not start_date:
        years = [int(year) for year in hcfg.YEARS] if not years else years
    
    BT.run_hist_file_routine(symbols=symbols, years=years, days=days, time_period=time_period,
        market_data='trades', start_date=start_date, end_date=end_date)
    return None

def get_klines(symbols:list,years:list=None, months:list=None,interval:list=['1m'], 
        days:list=None, time_period:str='monthly', start_date:str=None, end_date:str=None):
    BT = BinanceTrooper()
    years = hcfg.YEARS if not years else years
    for intval in interval:
        BT.run_hist_file_routine(symbols=symbols, years=years, days=days, time_period=time_period,
            market_data='klines', start_date=start_date, end_date=end_date, interval=intval)
    return None

def get_aggTrades(symbols:list,years:list=None, months:list=None, 
        days:list=None, time_period:str='monthly', start_date:str=None, end_date:str=None):
    BT = BinanceTrooper()
    years = hcfg.YEARS if not years else years
    BT.run_hist_file_routine(symbols=symbols, years=years, days=days, time_period=time_period,
        market_data='aggTrades', start_date=start_date, end_date=end_date)
    return None

if __name__ == '__main__':
    
    # standardized routine 
    symbols = BinanceTrooper().retrieve_symbols_list()
    
    base_filter = 'ETH'
    symbols = [x for x in symbols if x[:3] == base_filter]
    # trades
    get_trades(symbols=symbols, time_period='monthy')

    # klines
    #get_klines(symbols=symbols, time_period='monthly', interval=['1m', '5m', '1h'])

    # aggTrades
    #get_aggTrades(symbols=symbols, time_period='monthly')

    # if you are interested for specific pair for specific date range
    #symbols = ['BTCUSD']
    #start_date = '2021-10-21'
    #end_date = '2021-10-21'
    # trades
    #get_trades(symbols=symbols, time_period='daily', start_date=start_date, end_date=end_date)


