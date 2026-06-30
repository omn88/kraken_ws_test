FROM python:3.12-slim

WORKDIR /app

# Install dependencies first so this layer is cached on source-only changes
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "pytest", "tests/", "-v"]
