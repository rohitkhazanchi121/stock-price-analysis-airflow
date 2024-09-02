# Use the official Airflow image as a base
FROM apache/airflow:2.8.1

# Copy your requirements.txt file to the Docker container
COPY ./requirements.txt /requirements.txt

# Install the Python dependencies
RUN pip install --no-cache-dir -r /requirements.txt