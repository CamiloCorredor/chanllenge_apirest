from fastapi import FastAPI, HTTPException, Depends
from pydantic import BaseModel, field_validator, Field
import psycopg2
import os
import pandas as pd
from typing import List
from datetime import datetime
from migration import SQL, security

# def get_db_connection():
#     return psycopg2.connect(
#         host=os.getenv("DB_HOST", "localhost"),
#         database=os.getenv("DB_NAME", "mydatabase"),
#         user=os.getenv("DB_USER", "myuser"),
#         password=os.getenv("DB_PASSWORD", "mypassword"),
#     )

path_security = '/home/camilo/Documentos/Globant_Challenge/sec-logs' #Mejorar con secret manager 
app = FastAPI()


class HiredEmployee(BaseModel):
    id: int
    name: str
    datetime: str
    department_id: int
    job_id: int

    @field_validator("datetime", mode="before")
    def validate_datetime(cls, value: str):
        Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00")) #Validate date
        except ValueError:
            Hermes.log_file(f'Value error. Detail = Invalid datetime format. Must be ISO 8601','ERROR')
            raise ValueError("Invalid datetime format. Must be ISO 8601.")
        return value

class Department(BaseModel):
    id: int
    department: str

class Job(BaseModel):
    id: int
    job: str

class InsertDataRequest(BaseModel):
    hired_employees: List[HiredEmployee] = Field(default_factory=list)
    departments: List[Department] = Field(default_factory=list)
    jobs: List[Job] = Field(default_factory=list)

    @field_validator("hired_employees", "departments", "jobs", mode="before")
    def validate_batch_size(cls, value):
        if not (1 <= len(value) <= 1000):
            Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
            Hermes.log_file(f'Each batch must contain between 1 and 1000 records.','ERROR')
            raise ValueError("Each batch must contain between 1 and 1000 records.")
        return value

@app.post("/insert_data")
def insert_data(request: InsertDataRequest):
    Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
    parameters = Hermes.config_file_read()
    connector = SQL(parameters['host'],
                         parameters['DB_name'],
                         parameters['usr'],
                         parameters['psw'],
                         parameters['schema'])

    conn = connector.connect()
    cursor = conn.cursor()
    try:
        #department
        department_insert = 0
        for department in request.departments:
            cursor.execute("INSERT INTO departments (id, department) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", (department.id, department.department))
            department_insert += 1 
        Hermes.log_file(f'It has inserted {department_insert} rows', 'INFO')
        #jobs
        jobs_insert = 0
        for job in request.jobs:
            cursor.execute("INSERT INTO jobs (id, job) VALUES (%s, %s) ON CONFLICT (id) DO NOTHING", (job.id, job.job))
            jobs_insert += 1
        Hermes.log_file(f'It has inserted {jobs_insert} rows', 'INFO')
        
        # Validate deparments and jobs
        cursor.execute("SELECT id FROM departments")
        valid_departments = {row[0] for row in cursor.fetchall()}
        
        cursor.execute("SELECT id FROM jobs")
        valid_jobs = {row[0] for row in cursor.fetchall()}

        for employee in request.hired_employees:
            if employee.department_id not in valid_departments:
                Hermes.log_file(f'Status code 400. Detail = Department ID {employee.department_id} does not exist','ERROR')
                raise HTTPException(status_code=400, detail=f"Department ID {employee.department_id} does not exist.")
                
            if employee.job_id not in valid_jobs:
                Hermes.log_file(f'Status code 400. Detail = Job ID {employee.job_id} does not exist','ERROR')
                raise HTTPException(status_code=400, detail=f"Job ID {employee.job_id} does not exist.")
            
            cursor.execute("""
                INSERT INTO hired_employees (id, name, datetime, department_id, job_id)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO NOTHING """, 
                (employee.id, employee.name, employee.datetime, employee.department_id, employee.job_id))
        
        conn.commit()
        Hermes.log_file('Data inserted successfully', 'INFO')
        return {"message": "Data inserted successfully"}
    except Exception as e:
        conn.rollback()
        Hermes.log_file(f'Status code 500. Detail = Dstr{e}','ERROR')
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        cursor.close()
        conn.close()
