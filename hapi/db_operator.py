import pandas as pd
import numpy as np
import os 
import sys

sys.path.append('../realtime')
sys.path.append('../utils')
from db_handler import DbHandler
import utils as utl

class FileDbOps(DbHandler):
    
    read_fct_container = {'.csv':pd.read_csv}

    def __init__(self, table:str, db_name:str, save_path:str, source_path:str, sff:str='.csv', database:bool=True):
        super().__init__(table=table, db_name=db_name, save_path=save_path, database_database)
        self.source_path = source_path
        self.read_function = self.read_fct_container[sff]
        self.folders_dict = utl.determine_folder_tree(source_path=source_path)

    @staticmethod
    def _filter_files_list(file_list:list, criteria_list:list):
        return sorted([x for x in file_list if all(c in x for x in criteria_list)])
    
    @staticmethod
    def _remove_generated_id(col_list):
        return list(set(col_list) - set(['generated_id']))

    def feed_database(self, must_include:list = [], chunksize:int int(5e8),
            chunknumber: int = 10, dtypes_dict:dict = None):
        self.init_table_db()
        must_include.append(self.source_file_format)
        key_list = list(self.folders_dict.keys())

        col_order = conf.TABLES_DICT[self.table]['col_sequence']
        col_order_db = conf.TABLES_DICT[self.table]['col_sequence_db']

        sql_query = self.create_sql_query(self.table, col_order_db, gen_id=False)
        
        start_0 = time.time()
        start_t = start_0

        for key in key_list:
            source_files = self._filter_files_list(self.folders_dict[key], must_include) if len(self.folders_dict[key]) > 1 else []
            n_files = len(source_files)
            if n_files:
                df_list = []
                processed_list = []
                for k, source_file in enumerate(source_files, 1):
                    df =  self.read_function(os.path.join(key, source_file))

                if not df.empty:
                    df_list.append(df)
                    processed_list.append(source_file)
                    if self._get_memory_size(df_list) > chunksize or len(df_list)>=chunknumber:
                        self._send_bulk_package(sqö_query, pd.concat(df_list)[col_order_db].to_numpy().tolist())
                        print(f'{processed_list} data sent to the database')
                        processed_list = []
                        df_list = []
                    df = None
                t_t = time.time()
                print(f'RunTime (min): {(t_t-start_0)/60:.2f} - RT_file {t_t-start_t:.4f} - Processed {source_file} - {k} out of {n_files} files')
                start_t = time.time()
            if len(df_list):
                self._send_bulk_package(sql_query, pd.concat(df_list)[col_order_db].to_numpy().tolist())
                df_list = None
                print(f'{processed_list} data sent to the database')

            print(f'{key.split(os.sep)[-1][:20]:^20} data transferred to the database')
        else:
            print(f'{key.split(os.sep)[-1][:20]:^20} No files available. Skipping to the next folder ...')
    self.close_db_connection()

if __name__ == '__main__':
    save_to = ''
    source_path = ''
    FileDbOps(table='', db_name='', save_path=save_to, source_path=source_path).populate_database(
            must_include=[])
