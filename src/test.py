# import requests

# url = "http://127.0.0.1:8000/insert_data"
# data = {
#     "hired_employees": [
#         {"id": 1, "name": "Pedro Lopez", "datetime": "2023-05-10T14:30:00Z", "department_id": 2, "job_id": 1}
#     ],
#     "departments": [
#         {"id": 100, "department": "HRR"}
#     ],
#     "jobs": [
#         {"id": 111, "job": "CEOO"}
#     ]
# }

# response = requests.post(url, json=data)
# print(response.json())


##uvicorn api_rest:app --host 127.0.0.1 --port 8000 --reload


curl -X POST "http://127.0.0.1:8000/insert_data" \
-H "Content-Type: application/json" \
-d '{
    "jobs": [
        {
            "id": 101,
            "job": "Software Engineer"
        },
        {
            "id": 102,
            "job": "Data Scientist"
        }
    ]
}'


curl -X POST "http://127.0.0.1:8000/insert_data" \
-H "Content-Type: application/json" \
-d '{
    "hired_employees": [
        {
            "id": 1,
            "name": "John Doe",
            "datetime": "2024-03-25T14:30:00Z",
            "department_id": 190,
            "job_id": 101
        }
    ]
}'



curl -X POST "http://127.0.0.1:8000/insert_data" \
-H "Content-Type: application/json" \
-d '{
    "departments": [
        {
            "id": 190,
            "department": "IT"
        }
    ]
}'



