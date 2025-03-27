Proyecto FastAPI con Docker

Este proyecto implementa una API REST usando FastAPI y la despliega en un contenedor Docker.

Requisitos previos

Python 3.8.20

Docker

pip

Instalación y Configuración

1. Clonar el repositorio

git clone <pendiente>
cd <pendiente>

2. Se recomienda crear y activar un entorno virtual 

python3 -m venv venv
source venv/bin/activate  # En Linux/Mac
venv\Scripts\activate  # En Windows

3. Instalar dependencias

pip install -r requirements.txt

Construcción y ejecución con Docker

1. Construir la imagen Docker

sudo docker build -t fastapi-app .

2. Ejecutar el contenedor

sudo docker run -d -p 8000:8000 --name fastapi_container fastapi-app

3. Verificar que el contenedor esté corriendo

sudo docker ps

Uso de la API

1. Probar la API con curl

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

Detener y eliminar el contenedor

1. Detener el contenedor

sudo docker stop fastapi_container

2. Eliminar el contenedor

sudo docker rm fastapi_container

Licencia

Este proyecto está bajo la licencia MIT.

