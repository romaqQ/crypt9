import time
import datetime as dt
import numpy as np
import aiosqlite as sql
import sqlite3 as sql3
import os
import sys
import ujson as json
import asyncio

import config as cfg
import channels as chnl
import exchange as ex

sys.path.append('../lib')
import utils as utl

class DbHandler:

    def __init__(self, exchange:str=None, table:str = None, channel:str = None,
            db_name:str = None, db_dir: str = None, database:bool = True):
        self.table = self.uniform_table_name(table)
        self.exchange= exchange
        self.channel = channel
        self.root_folder = cfg.DIR_STRUCTURE['root']
        self.destination_path = os.path.join(self.root_folder,'data')
        self.db_dir = 'realtime' if not db_dir else db_dir
        if database:
            self._init_db_path(db_name=db_name)

    def _init_db_path(self, db_name:str=None):
        self.db_path = os.path.join(self.destination_path, self.db_dir, self.exchange)
        utl.create_dir(self.db_path)
        if db_name:
            self.db_file_path = os.path.join(self.db_path, f'{db_name}.sqlite')
        else:
            self.db_file_path = os.path.join(self.db_path, f'{self.channel}.sqlite')
    
    @staticmethod
    def uniform_table_name(table):
        return table.replace('/','').replace('-','').replace('_','')

    async def _establish_connection(self):
        self.conn = await sql.connect(self.db_file_path)
        self.cursor = await self.conn.cursor()
    
    async def execute(self, query:str, fetch:bool=False, commit:bool=False):
        await self._establish_connection()
        #await asyncio.sleep(cfg.ASYNC_SLEEP)
        result = await self.cursor.execute(query)
        if fetch:
            result = await result.fetchall()
        if commit:
            await self.conn.commit()
        await self.conn.close()
        return result
    
    async def executemany(self, sql_query, ordered_list_of_tuples):
        await self._establish_connection()
        #await asyncio.sleep(cfg.ASYNC_SLEEP)
        try:
            await self.cursor.executemany(sql_query, ordered_list_of_tuples)
        except:
            breakpoint()
        await self.conn.commit()
        await self.conn.close()
   
    def init_table_db(self):
        all_tables = "select name from sqlite_master where type='table';"
        conn = sql3.connect(self.db_file_path)
        cursor = conn.cursor()
        names = [x[0].upper() for x in cursor.execute(all_tables).fetchall()]
        if self.table.upper() in names:
            pass
        else:
            cursor.execute(self.table_create_query)
            time.sleep(cfg.ASYNC_SLEEP)
            conn.commit()
        conn.close()

    @classmethod
    def create_sql_query(cls, table:str, cols:list, gen_id:bool=False):
        if gen_id:
            return 'insert into {0} values (null{1})'.format(table, ',?'*len(cols))
        return 'insert into {0} values (?{1})'.format(table, ',?'*(len(cols)-1))
    
    @classmethod
    def retrieve_columns_from_query(cls, sql_string:str):
        return [x.strip().split(']')[0].replace('[','').replace('\n', '') for x in sql_string.split('(')[1].replace(')', '').split(',')]
 
    
class RealTimePeon(DbHandler):

    def __init__(self, exchange:str='binance', symbol:str = 'btcusdt', channel:str = 'trade',
            db_name:str = None,db_dir:str=None, database:bool=True):
        super().__init__(table=symbol, channel=channel, exchange=exchange,
                db_name=db_name, db_dir=db_dir, database=database)
        self.table_create_query = self.load_query(table=self.table)
        self.runtime_start = None
        self.parsed_channel = ex.CHANNEL_MAPPING[channel][exchange]
        self._set_parsing_function()
    
    def load_query(self, table:str=None):
        table = f'"{table}"'
        query = chnl.CHANNELS_DICT[self.channel][self.exchange]['table'].format(table)
        self.col_order = self.retrieve_columns_from_query(query)
        # //ToDo: incorporate mechanism to handle different columns from input to database storage
        self.col_order_db = self.col_order
        return query
    
    def _set_parsing_function(self):
        self.parse_formats = chnl.CHANNELS_DICT[self.channel][self.exchange]['parsing_function']
 
    async def queue_batches(self, batchsize:float=1e6, batchlen:int = 100, ws_upstream:object = None, ws_downstream:object=None):
        batch_container = []
        batch_len = 0
        batch_size = 0
        self.runtime_start = time.time()
        try:
            while True:
                data_increment = await ws_upstream.get()
                if data_increment:
                    batch_container.append(data_increment) if not 'heartbeat' in data_increment else None
                else:
                    if batch_len > 0:
                        await ws_downstream.put(batch_container)
                        batch_container = []
                        print(f'Time: {time.strftime("%X")} - Runtime: {(time.time()-self.runtime_start)/60:.2f} min - {self.table} - batch of length {batch_len} and {batch_size/1e6:.3f} mb size sent to downstream processing.')
                        break
                batch_size = utl.get_memory_size(batch_container)
                batch_len = len(batch_container)
                if any([batch_size >= batchsize, batch_len>= batchlen]):
                    await ws_downstream.put(batch_container)
                    batch_container = []
                    print(f'Time: {time.strftime("%X")} - Runtime: {(time.time()-self.runtime_start)/60:.2f} min - {self.table} - batch of length {batch_len} and {batch_size/1e6:.3f} mb size sent to downstream processing.')
            await ws_downstream.put(None)
        except KeyboardInterrupt:
            if batch_len > 0:
                await ws_downstream.put(batch_container)
                batch_container = []
                print(f'Time: {time.strftime("%X")} - Runtime: {(time.time()-self.runtime_start)/60:.2f} min - {self.table} - batch of length {batch_len} and {batch_size/1e6:.3f} mb size sent to downstream processing.')
                await ws_downstream.put(None)
    
    @classmethod
    def prepare_batches(cls, batch_container):
        return [json.loads(x) for x in batch_container if len(x) > 40]

    async def stream_to_database(self, batchsize:float = 4e5, batchlen: int = 250, ws_upstream:object = None):
        sql_query = self.create_sql_query(table=self.table, cols=self.col_order_db)
        batch_container = []
        batch_len = 0
        batch_size = 0

        try:
            while True:
                data_batches = await ws_upstream.get()
                if data_batches:
                    batch_container.extend(self.prepare_batches(data_batches))
                    batch_len = len(batch_container)
                    batch_size = utl.get_memory_size(batch_container)
                    if any([batch_len>=batchlen, batch_size>=batchsize]):
                        await self.executemany(sql_query, self.parse_formats(batch_container))
                        batch_container = []
                        print(f'Time: {time.strftime("%X")} - {self.table} - sent batch of length {batch_len} and {batch_size/1e6:.3f} mb size to db.')
                else:
                    if batch_len > 0:
                        await self.executemany(sql_query, self.parse_formats(batch_container))
                        batch_container = []
                        print(f'Time: {time.strftime("%X")} - {self.table} - sent batch of length {batch_len} and {batch_size/1e6:.3f} mb size to db.')
                        print(f'Database Streaming Terminated')
                        break
                    else:
                        break
        except KeyboardInterrupt:
            if batch_len > 0:
                await self.executemany(sql_query, self.parse_formats(batch_container))
                batch_container = []
                print(f'Time: {time.strftime("%X")} - {self.table} - sent batch of length {batch_len} and {batch_size/1e6:.3f} mb size to db.')
                print(f'Database Streaming Terminated')

