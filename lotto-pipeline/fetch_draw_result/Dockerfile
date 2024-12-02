# build image for each task that Airflow Kubernetes operator calls
FROM python:3.9.20-slim-bullseye

# Set working directory
WORKDIR /app

# Copy dependenies list
COPY requirements.txt /app/requirements.txt
# Install dependencies
RUN pip install -r /app/requirements.txt

# Set the default command
CMD ["python", "app.py"]

# Copy the Python script
COPY app.py /app/
