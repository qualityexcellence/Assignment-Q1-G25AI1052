FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p keys

EXPOSE 5001 5002 5003 5004 5005

CMD ["python", "src/node.py", "1"]