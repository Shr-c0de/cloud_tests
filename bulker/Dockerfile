FROM python:3.13-slim

COPY bulker/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY database_pb2.py .
COPY database_pb2_grpc.py .
COPY ./bulker/bulker.py .


CMD ["python", "bulker.py"]