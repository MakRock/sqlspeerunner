import pandas as pd
import time
import psycopg2
import tempfile
import io
import numpy as np
import multiprocessing as mp
from multiprocessing import Pool
import os
import math
import warnings
warnings.filterwarnings("ignore")

class Run:
    def __init__(self, base, schema):
        self.schema = schema
        self.conn = psycopg2.connect(database=base, 
                                      user=os.environ['user'], 
                                      password=os.environ['password_db'], 
                                      host="your_host", 
                                      port="6432",
                                      sslmode='verify-full')
        with self.conn.cursor() as cursor:
            cursor.execute('SELECT current_database ();')
            print(f'Вы подключены к базе: {cursor.fetchone()[0]}')
            print(f'Проект (изм. 4): {self.schema}')
            cursor.execute('SELECT version()')
            print(f'Хост: {cursor.fetchone()}')
        
        
    def get(self, table_name='', nrows='all', where='', cols='*', query='select'):
        limit = ['limit ' + str(nrows) if nrows != 'all' else ''][0]
        cols_query = ', '.join(cols)  
        if query == 'select':
            query = f"""SELECT {cols_query} FROM {self.schema}.{table_name} {where} {limit}"""
        sql = f"COPY ({query}) TO STDOUT WITH CSV HEADER"
        with tempfile.TemporaryFile() as tmpfile:
            with self.conn.cursor() as cursor:
                cursor.copy_expert(sql, tmpfile)
                tmpfile.seek(0)
                data = pd.read_csv(tmpfile, dtype={'munr_oktmo':str, 'settlement_oktmo':str, 'sub_oktmo':str})
        return data
        
        
    def put(self, data, table_name, default_len_str='auto', encoding='cp1251'):
        if isinstance(data, str):
            path = data
            data = pd.read_csv(path, nrows=100)
            create_table_query = create_query(data, self.schema, table_name, default_len_str)
            export_data_query = f"COPY {self.schema}.{table_name} FROM STDIN DELIMITER ',' CSV HEADER"
            data_buffer = open(path, "r", encoding=encoding)
        else:
            create_table_query = create_query(data, self.schema, table_name, default_len_str)
            export_data_query = f"COPY {self.schema}.{table_name} FROM STDIN DELIMITER ',' CSV HEADER"
            data_buffer = create_data_buffer(data)
        start_time = time.time()
        with self.conn.cursor() as cursor:
            cursor.execute(create_table_query)
            cursor.execute("COMMIT")
            cursor.copy_expert(export_data_query, data_buffer)
            cursor.execute("COMMIT")
            data_buffer.close()
        end_time = time.time() - start_time
        return print(f'Экспорт в базу завершен за {timing(end_time)}')

            
    
    def status(self):
        if self.conn.closed == 0:
            print('Коннект открыт')
        else:
            print('Коннект закрыт')
    
    
    def conn_close(self):
        self.conn.close()
        if self.conn.closed == 0:
            print('Коннект открыт')
        else:
            print('Коннект закрыт')
        

def sql_type(row, len_objects):
    type_c = row['type']
    column = row['column']
    if type_c == 'int64':
        return column + ' integer NULL'
    elif type_c == 'float64':
        return column + ' float NULL'
    elif type_c == 'object':
        len_s = len_objects[column]
        return column + f' varchar({len_s})'
    elif type_c == 'bool':
        return column + ' bool'


def timing(time):
    if time < 1:
        return str(round(time, 1)) + ' сек'
    if time < 60:
        return str(round(time, )) + ' сек'
    if time > 60:
        time = int(time)
        min_cnt = time//60
        sek_cnt = time - (min_cnt * 60)
        if sek_cnt < 10:
            sek_cnt = '0' + str(sek_cnt)
        return f'{min_cnt}:{sek_cnt} мин'

        
def object_len(data, default_len_str):
    if default_len_str == 'auto':
        measurer = np.vectorize(len)
        data_object = data.select_dtypes(include=[object])
        if data_object.shape[1] == 0:
            len_objects = {}
            return len_objects
        len_objects = dict(zip(data_object, measurer(data_object.values.astype(str)).max(axis=0)))
        for column, lenstr in len_objects.items():
            len_objects[column] = math.ceil(lenstr/10) * 10
        return len_objects
    else:
        len_objects = dict.fromkeys(data.select_dtypes(include=[object]).columns, default_len_str)
        return len_objects
    

def create_query(data, schema, table_name, default_len_str):    
    tmp = data.dtypes.reset_index().rename(columns={'index':'column', 0:'type'})
    start_time = time.time()
    len_objects = object_len(data, default_len_str)
    len_time = time.time() - start_time
#     print(f'На получение max длины строк потрачено {timing(len_time)}')
    tmp['query'] = tmp.apply(sql_type, axis=1, len_objects=len_objects)
    fields = ', '.join(tmp['query'].tolist())
    query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} ({fields});"
    return query

   

def df_to_string(df):
    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False)
    buf.seek(0)
    s = buf.read()
    buf.close()
    return s


def df_to_string_parallel(data):
    n_cores = mp.cpu_count()
    pool = Pool(n_cores)
    df_split = np.array_split(data, n_cores)
    try:
        list_str = pool.map(df_to_string, df_split)
    except:
        print('Ошибка параллелизации')
    finally:
        pool.close()
        pool.join()
    return list_str


def parallel_process(data):
    data.columns.tolist()
    list_str = df_to_string_parallel(data)
    df_as_long_str = ''.join(list_str)
    return df_as_long_str


def create_data_buffer(data):
    df_as_long_str = parallel_process(data)
    buffer = io.StringIO()
    data.iloc[0:0].to_csv(buffer, index=False)
    buffer.write(df_as_long_str)
    buffer.seek(0)
    return buffer
