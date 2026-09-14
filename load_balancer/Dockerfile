FROM python:3.13-slim

WORKDIR /app

ENV PATH="/usr/lib/postgresql/18/bin:$PATH"

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY load_balancer.py .

CMD ["uvicorn", "load_balancer:load", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]