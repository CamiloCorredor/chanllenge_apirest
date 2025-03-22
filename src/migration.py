import psycopg2
import pandas as pd
import json
import os
from openpyxl import load_workbook
from datetime import datetime
from fastapi import FastAPI, HTTPException

path_security = '/home/camilo/Documentos/Globant_Challenge/sec-logs'

class security:
       
    def __init__(self, config_file, path_file="/home/camilo/Documentos/Globant_Challenge/sec-logs/logfile.log"):
        # self.today = today
        self.path_file = path_file 
        os.makedirs(os.path.dirname(self.path_file), exist_ok=True)          
        self.config_file = config_file 

    def log_file(self, message, level):        
        now = datetime.now()
        timestamp = now.strftime('%Y-%m-%d %H:%M:%S')
        log_entry = f"{timestamp} - {level} - {message}"
        
        try:
            with open(self.path_file, 'a') as file:
                file.write(log_entry + '\n')
        except Exception as e:
            print(f"Error al escribir en el log: {e}")
            
    def config_file_read(self):
        with open(self.config_file, 'r') as f:
            lines = f.readlines()
            parametros = {}
            for line in lines:
                key, value = line.strip().split('=')
                parametros[key] = value

        return parametros

class SQL_LADM:    

    def __init__(self, host, database, user, psw, schema):
        self.host = host
        self.database = database
        self.user = user
        self.psw = psw        
        self.schema = schema
        
    def connect(self):
        
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        try: 
            connection = psycopg2.connect(
            host=self.host,
            database=self.database,
            user=self.user,
            password=self.psw
            )
            Hermes.log_file('Connection established', 'INFO')
            return connection
        except Exception as e:
            Hermes.log_file('Failed connection to database', 'ERROR')
            
    def load_csv_to_db(self, file_path: str, table_name: str):
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        conn = self.connect()
    
        try:
            df = pd.read_csv(file_path)
            for _, row in df.iterrows():
                conn.execute(
                    f"INSERT INTO {table_name} VALUES ({', '.join(['%s'] * len(row))})",
                tuple(row)
                )
            conn.commit()
        except Exception as e:
            conn.rollback()
            Hermes.log_file(f"Error loading CSV to {table_name}: {str(e)}", 'ERROR')
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            conn.close()            
            Hermes.log_file(f"Data from {file_path} loaded into {table_name}", 'INFO')
            

            
            
Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
parameters = Hermes.config_file_read()
connector = SQL_LADM(parameters['host'],
                         parameters['DB_name'],
                         parameters['usr'],
                         parameters['psw'],
                         parameters['schema'])


connector.connect()
            
            
