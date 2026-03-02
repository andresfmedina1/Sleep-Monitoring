FROM python:3.11-slim

WORKDIR /app

# Reqs del microservicio de alarmas
COPY alarm/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código compartido y del servicio
COPY common ./common
# Si tu entrypoint es alarm.py, copia con ese nombre:
COPY alarm/alarm.py .
COPY alarm/settings.json .

CMD ["python", "-u", "alarm.py"]


