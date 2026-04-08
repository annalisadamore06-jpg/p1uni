FROM python:3.11-slim

WORKDIR /app

# Dipendenze sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && rm -rf /var/lib/apt/lists/*

# Dipendenze Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Codice
COPY src/ src/
COPY main.py .
COPY config/settings.example.yaml config/settings.example.yaml

# Directories
RUN mkdir -p data/quarantine data/cache logs ml_models

# Entry point
ENTRYPOINT ["python", "main.py"]
CMD ["--mode", "paper"]
