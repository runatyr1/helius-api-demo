FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

EXPOSE 8000

CMD ["python", "-u", "producer.py"]
