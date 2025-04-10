FROM python:3.12-slim

WORKDIR /App

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_app_async.py .

CMD ["python", "mqtt_app_async.py"]