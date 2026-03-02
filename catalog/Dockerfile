FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY catalog.py .
# montaremos el JSON como volumen en /data
ENV PORT=9080 CATALOG_PATH=/data/catalog.json
CMD ["python","catalog.py"]
