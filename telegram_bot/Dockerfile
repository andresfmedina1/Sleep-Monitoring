FROM python:3.11-slim

WORKDIR /app

COPY telegram_bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common ./common
COPY telegram_bot/telegram_bot.py .
COPY telegram_bot/settings.json .

ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "telegram_bot.py"]
