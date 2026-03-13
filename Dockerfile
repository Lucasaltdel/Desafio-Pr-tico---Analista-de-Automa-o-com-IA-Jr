FROM python:3.14.3-slim

WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "api_mock:app", "--host", "0.0.0.0", "--port", "8000"]