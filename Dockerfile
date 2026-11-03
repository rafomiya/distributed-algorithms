FROM python:3.10
RUN apt-get update && \
    apt-get install -y --no-install-recommends iproute2 iptables curl && \
    rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ /app/
CMD ["python", "-m", "node.main"]
