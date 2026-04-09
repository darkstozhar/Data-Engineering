FROM python:3.8-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY produce.py .

ENTRYPOINT ["python"]
CMD ["produce.py"]
