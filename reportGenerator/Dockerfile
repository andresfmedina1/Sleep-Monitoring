FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY reporting_service.py .
COPY settings.json .

EXPOSE 8093
CMD ["python3", "reporting_service.py"]

