import requests

url = "http://127.0.0.1:8000/insert_data"
data = {
    "hired_employees": [
        {"id": 1, "name": "Pedro Lopez", "datetime": "2023-05-10T14:30:00Z", "department_id": 2, "job_id": 1}
    ],
    "departments": [
        {"id": 100, "department": "HRR"}
    ],
    "jobs": [
        {"id": 111, "job": "CEOO"}
    ]
}

response = requests.post(url, json=data)
print(response.json())
