import psycopg2
import pandas as pd
import json
import os
from openpyxl import load_workbook
from datetime import datetime
from fastapi import FastAPI, HTTPException

path_security = '/home/camilo/Documentos/Globant_Challenge/sec-logs'
query = """
    SELECT 
    d.department AS department,
    j.jobs AS job,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 1 THEN 1 END) AS Q1,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 2 THEN 1 END) AS Q2,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 3 THEN 1 END) AS Q3,
    COUNT(CASE WHEN EXTRACT(QUARTER FROM he.datetime) = 4 THEN 1 END) AS Q4
FROM challenge.hired_employees he
JOIN challenge.departments d ON he.department_id = d.id
JOIN challenge.jobs j ON he.job_id = j.id
WHERE EXTRACT(YEAR FROM he.datetime) = 2021
GROUP BY d.department, j.jobs
ORDER BY d.department ASC, j.jobs ASC;
"""

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

class SQL:    

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
            #print(self.database, self.host)
            return connection
        except Exception as e:
            Hermes.log_file('Failed connection to database', 'ERROR')
            
    def load_csv_to_db(self, file_path: str, table_name: str):
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        conn = self.connect()
        cursor = conn.cursor()
        try:
            df = pd.read_csv(file_path, sep=',')
            # print(df)
            # cursor.execute(f"SELECT to_regclass('challenge.{table_name}')")
            # table_exists = cursor.fetchone()[0]
            # print(df.isnull().sum()) 
            df = df.applymap(lambda x: None if pd.isna(x) else x)
            if table_name == 'department' or table_name == 'jobs': 
                df.iloc[:, 0] = df.iloc[:, 0].fillna(0).astype(int)
            elif table_name == 'hired_employees':
                df.iloc[:, -2:] = df.iloc[:, -2:].fillna(0).astype(int) ###Bug fixed
            
            for _, row in df.iterrows():
                cursor.execute(
                    f"INSERT INTO challenge.{table_name} VALUES ({', '.join(['%s'] * len(df.columns))})",
                tuple(row)
                )
            conn.commit()
            Hermes.log_file(f"Data inserted on {table_name}", 'INFO')
        except Exception as e:
            Hermes.log_file(
                f"Error loading CSV to {table_name}: {e}.", 'ERROR')

            conn.rollback()
            raise HTTPException(status_code=400, detail=str(e))
        finally:
            cursor.close()            
            Hermes.log_file(f"Connection closed", 'INFO')
            
            
    def queries(self, query):
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        conn = self.connect()
        cursor = conn.cursor()
        try:
            df = pd.read_sql(query, conn)
            Hermes.log_file(f"Query ejecutada", 'INFO')
            return df  # ✅ Ahora la conexión se cerrará antes de salir
        except Exception as e:
            Hermes.log_file(f"Se ha encontrado un error al ejecutar Query: {e}.", 'ERROR')
            conn.rollback()
            return None
        finally:
            cursor.close()
            conn.close()
            Hermes.log_file(f"Connection closed", 'INFO')
        

            
            


if __name__ == "__main__": #Bug fixed 
    Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
    parameters = Hermes.config_file_read()
    connector = SQL(parameters['host'],
                         parameters['DB_name'],
                         parameters['usr'],
                         parameters['psw'],
                         parameters['schema'])
    connector.connect()
    # connector.load_csv_to_db('/home/camilo/Documentos/Globant_Challenge/data/departments.csv', 'departments')
    # connector.load_csv_to_db('/home/camilo/Documentos/Globant_Challenge/data/jobs.csv', 'jobs')
    # connector.load_csv_to_db('/home/camilo/Documentos/Globant_Challenge/data/hired_employees.csv', 'hired_employees')
    
    
    df = connector.queries(query)
    print(df)

            
            
