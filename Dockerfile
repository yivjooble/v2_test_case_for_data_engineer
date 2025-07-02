FROM apache/airflow:3.0.2

# Copy requirements file
COPY requirements.txt /requirements.txt

# Install additional requirements
RUN pip install --no-cache-dir -r /requirements.txt

# Copy utility_hub module to a location that won't be overridden by volume mounts
COPY /dags/utility_hub /opt/airflow/utility_hub

# Add utility_hub to Python path
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

# Switch back to airflow user
USER airflow