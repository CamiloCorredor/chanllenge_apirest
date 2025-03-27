import psycopg2
import pandas as pd
import fastavro
import os
from migration import security, SQL
path_security = os.path.join(os.getcwd(), "sec-logs")
path_security = '/home/camilo/Documentos/Globant_Challenge/sec-logs'

class Backup:
    def __init__(self, sql_connector, backup_dir):
        self.sql_connector = sql_connector
        self.backup_dir = backup_dir
        os.makedirs(self.backup_dir, exist_ok=True)

    def backup_table_2_avro(self, table_name):
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        conn = self.sql_connector.connect()
        cursor = conn.cursor()
        
        try:
            
            cursor.execute(f"SELECT * FROM challenge.{table_name}")
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]           
            df = pd.DataFrame(rows, columns=columns)
            if table_name == 'hired_employees':
                df['datetime'] = df['datetime'].astype(str)  ###Bug fixed
                

            if df.empty:
                Hermes.log_file(f"No hay registros en la tabla {table_name}.", 'INFO')
                return

            
            schema = {
                "type": "record",
                "name": f"{table_name}_record",
                "fields": [{"name": col, "type": ["null", "string", "int", "float", "double"]} for col in columns]
            }

            
            avro_file = os.path.join(self.backup_dir, f"{table_name}.avro")
            with open(avro_file, "wb") as out_file:
                writer = fastavro.writer(out_file, schema, df.to_dict(orient="records"))

            Hermes.log_file(f"Backup de latabla {table_name} guardado en {avro_file}", 'INFO')

        except Exception as e:
            Hermes.log_file(f"Error durante backup de la tabla {table_name}: {str(e)}", 'ERROR')

        finally:
            cursor.close()
            conn.close()
            
    def backup_2_DB(self, avro_file, table_name):
        
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        conn = self.sql_connector.connect()
        cursor = conn.cursor()
        
        try:
        # Leer los datos desde el archivo AVRO
            with open(avro_file, "rb") as f:
                reader = fastavro.reader(f)
                records = [record for record in reader]

            if not records:
                Hermes.log_file(f"El archivo {avro_file} está vacío. No hay datos para restaurar.", 'INFO')
        
            cursor = conn.cursor()
            columns = records[0].keys()
            columns_str = ", ".join(columns)
            placeholders = ", ".join(["%s"] * len(columns))

            cursor.execute(f"DELETE FROM challenge.{table_name}")

        
            for record in records:
                values = tuple(record[col] for col in columns)
                cursor.execute(
                    f"INSERT INTO challenge.{table_name} ({columns_str}) VALUES ({placeholders})",
                    values
                )

            conn.commit()
            cursor.close()
            conn.close()

            Hermes.log_file(f"La tabla '{table_name}' ha sido restaurada exitosamente desde {avro_file}.", 'INFO')

        except Exception as e:
            Hermes.log_file(f"Error al restaurar la tabla '{table_name}': {str(e)}",'INFO')


if __name__ == "__main__":
    Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
    parameters = Hermes.config_file_read()
    connector = SQL(parameters['host'],
                         parameters['DB_name'],
                         parameters['usr'],
                         parameters['psw'],
                         parameters['schema'])
    path_backup = "/home/camilo/Documentos/Globant_Challenge/backups"
    backup_manager = Backup(connector, path_backup)


    tables = ["departments", "jobs", "hired_employees"]
    for table in tables:
        path_avro = f"{path_backup}/{table}.avro"
        # backup_manager.backup_2_DB(path_avro, table)
        backup_manager.backup_table_2_avro(table)
