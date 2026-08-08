FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY bridge.py .

RUN useradd --create-home --uid 1000 bridge
USER bridge

CMD ["python", "-u", "bridge.py"]