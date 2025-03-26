from fastapi import FastAPI, HTTPException
from typing import Optional, List
from pydantic import BaseModel, field_validator, Field, model_validator
import psycopg2
import os
from typing import List
from datetime import datetime
from migration import SQL, security
import traceback

path_security = '/home/camilo/Documentos/Globant_Challenge/sec-logs'  # Mejorar con secret manager
Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')

app = FastAPI()

class HiredEmployee(BaseModel):
    id: Optional[int] = None
    name: str = Field(..., min_length=1)
    datetime: str = Field(...)
    department_id: int = Field(..., gt=0)
    job_id: int = Field(..., gt=0)

    @field_validator("datetime", mode="before")
    def validate_datetime(cls, value):
        
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))  # Validar fecha
        except ValueError:
            Hermes.log_file(f'Value error. Detail = Invalid datetime format. Must be ISO 8601', 'ERROR')
            raise ValueError("Invalid datetime format. Must be ISO 8601.")
        return value

class Department(BaseModel):
    id: int = Field(..., gt=0)  
    department: str = Field(..., min_length=1)  
    
    @field_validator("department", mode="before")
    def validate_department(cls, value: str):
        
        if not isinstance(value, str) or not value.strip():
            Hermes.log_file(f'Validation error: Department name cannot be empty', 'ERROR')
            raise ValueError("Department name cannot be empty.")
        return value

class Job(BaseModel):
    id: int = Field(..., gt=0)
    job: str = Field(..., min_length=1)

    @field_validator("job", mode="before")
    def validate_job(cls, value):
        if not isinstance(value, str) or not value.strip():
            Hermes.log_file(f'Validation error: Job title cannot be empty', 'ERROR')
            raise ValueError("Job title cannot be empty.")
        return value

class InsertDataRequest(BaseModel):  ##Bug fixed
    hired_employees: Optional[List[HiredEmployee]] = Field(default_factory=list)  
    departments: Optional[List[Department]] = Field(default_factory=list)  
    jobs: Optional[List[Job]] = Field(default_factory=list)

@app.post("/insert_data")
async def insert_data(request: InsertDataRequest):
    Hermes = security(f'{path_security}/configfile.txt', f'{path_security}/logfile.log')
    conn = None
    cursor = None

    try:
        print("Datos recibidos:", request.dict())
        Hermes.log_file("Processing insert_data request", "INFO")

        parameters = Hermes.config_file_read()
        connector = SQL(parameters['host'],
                        parameters['DB_name'],
                        parameters['usr'],
                        parameters['psw'],
                        parameters['schema'])

        conn = connector.connect()
        cursor = conn.cursor()

        # Insertar departamentos
        
        department_inserted = 0
        for department in request.departments:
            try:
                cursor.execute(
                       "INSERT INTO challenge.departments (id, department) VALUES (%s, %s)",
                        (department.id, department.department)
                        )
                department_inserted += 1
            except psycopg2.IntegrityError as e:
                conn.rollback()
                Hermes.log_file(f"IntegrityError PK {department.id}: {e}. Request: ID={department.id}, department={department.department}", "ERROR")
            except psycopg2.DatabaseError as e:
                conn.rollback()
                Hermes.log_file(f"DatabaseError inserting department {department.department}: {e}", "ERROR")

            Hermes.log_file(f'Inserted {department_inserted} departments', 'INFO')

        
    
        # Insertar trabajos
        jobs_inserted = 0
        for job in request.jobs:
            try:
                cursor.execute(
                "INSERT INTO challenge.jobs (id, jobs) VALUES (%s, %s)",
                (job.id, job.job)
                )
                jobs_inserted += 1
            except psycopg2.IntegrityError as e:
                conn.rollback()
                Hermes.log_file(f"IntegrityError PK {job.id}: {e}. Request: ID={job.id}, department={job.job}", "ERROR")
            except psycopg2.DatabaseError as e:
                conn.rollback()
                Hermes.log_file(f"DatabaseError inserting job job {job.job}: {e}", "ERROR")

        Hermes.log_file(f'Inserted {jobs_inserted} jobs', 'INFO')

        # Obtener IDs válidos de departamentos y trabajos
        cursor.execute("SELECT id FROM challenge.departments")
        valid_departments = {row[0] for row in cursor.fetchall()}

        cursor.execute("SELECT id FROM challenge.jobs")
        valid_jobs = {row[0] for row in cursor.fetchall()}

        # Insertar empleados
        employees_inserted = 0
        for employee in request.hired_employees:
            if employee.department_id not in valid_departments:
                msg = f"Department ID {employee.department_id} does not exist."
                Hermes.log_file(f"Status code 400. Detail = {msg}", 'ERROR')
                Hermes.log_file(f"Data not inserted {request.hired_employees}")
                raise HTTPException(status_code=400, detail=msg)

            if employee.job_id not in valid_jobs:
                msg = f"Job ID {employee.job_id} does not exist."
                Hermes.log_file(f"Data not inserted {request.hired_employees}")
                Hermes.log_file(f"Status code 400. Detail = {msg}", 'ERROR')
                raise HTTPException(status_code=400, detail=msg)
            
            cursor.execute("SELECT MAX(ID)+1 FROM challenge.hired_employees;")  ##Manejo de id en la tabla
            new_id = cursor.fetchone()[0]

            cursor.execute(
                """
                INSERT INTO challenge.hired_employees (id, name, datetime, department_id, job_id)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING id; 
                """,
                (new_id, employee.name, employee.datetime, employee.department_id, employee.job_id)
            )
            employees_inserted += 1

        conn.commit()
        Hermes.log_file(f'Inserted {employees_inserted} employees', 'INFO')

        return {"message": "Data inserted successfully"}

    except psycopg2.DatabaseError as db_err:
        if conn:
            conn.rollback()
        Hermes.log_file(f"Database error: {db_err}", "ERROR")
        raise HTTPException(status_code=500, detail="Database error occurred.")

    except Exception as e:
        if conn:
            conn.rollback()
        Hermes.log_file(f"Unexpected error: {traceback.format_exc()}", "ERROR")
        raise HTTPException(status_code=500, detail="Internal server error.")

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        Hermes.log_file("Database connection closed", "INFO")
