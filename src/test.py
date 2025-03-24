import requests

url = "http://127.0.0.1:8000/insert_data"
data = {
    "hired_employees": [
        {"id": 1, "name": "Pedro Perez", "datetime": "2023-05-10T14:30:00Z", "department_id": 2, "job_id": 1}
    ],
    "departments": [
        {"id": 1, "department": "HR"}
    ],
    "jobs": [
        {"id": 1, "job": "CEO"}
    ]
}

response = requests.post(url, json=data)
print(response.json())
