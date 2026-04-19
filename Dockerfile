FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY multi-agent-bridge.py .

CMD ["python", "-u", "multi-agent-bridge.py"]
