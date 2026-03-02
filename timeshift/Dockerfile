FROM python:3.11-slim

WORKDIR /app

# Reqs del servicio
COPY timeshift/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Código compartido y del servicio
COPY common ./common
COPY timeshift/timeshift.py .
COPY timeshift/settings.json .

CMD ["python", "timeshift.py"]
