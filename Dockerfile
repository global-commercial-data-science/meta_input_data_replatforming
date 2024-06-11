FROM python:3.9-slim

WORKDIR /app


COPY requirements.txt /app/

RUN pip install --no-cache-dir -r requirements.txt

COPY . /app

# Run the application
CMD ["python", "DAXmainProcess.py"]


LABEL authors="Vipul.Kumar"