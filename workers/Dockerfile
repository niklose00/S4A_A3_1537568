# Basis-Image: Leicht, stabil, Python 3.11
FROM python:3.11-slim

# Arbeitsverzeichnis im Container
WORKDIR /app

# Requirements (Kafka-Client, JSON etc.)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Kopiere den Worker-Code ins Image
COPY segment_worker.py .

# Standard-EntryPoint → Der Worker startet
CMD ["python", "segment_worker.py"]
