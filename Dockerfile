FROM python:3.8.20

WORKDIR /app
RUN apt-get update && apt-get install -y build-essential libpq-dev

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade pip setuptools wheel

RUN pip install --no-cache-dir -r requirements.txt


COPY src/api_rest.py .src/api_rest.py
COPY src/migration.py .src/migration.py
COPY sec-logs/configfile.txt ./sec-logs/configfile.txt

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000
##Lanzar la api
CMD ["uvicorn", "api_rest:app", "--host", "0.0.0.0", "--port", "8000", "--reload"] 

