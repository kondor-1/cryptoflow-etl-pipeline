# Dockerfile
# ===========
# Extends the official Airflow image with our project dependencies.
#
# 📚 CONCEPT: Why extend instead of build from scratch?
#   The official apache/airflow image already has Airflow installed
#   and configured correctly. We just need to add our requirements
#   on top of it. This keeps our Dockerfile simple and maintainable.

FROM apache/airflow:2.9.0-python3.11

# Switch to root to install system dependencies if needed
USER root

# Switch back to airflow user before installing Python packages
# Never install pip packages as root in a Docker container
USER airflow

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir \
    requests>=2.31.0 \
    tenacity>=8.2.0 \
    pandas>=2.0.0 \
    sqlalchemy>=2.0.0 \
    psycopg2-binary>=2.9.0 \
    python-dotenv>=1.0.0

# Copy the ETL modules into the image so Airflow can import them
COPY etl/ /opt/airflow/etl/
COPY dags/ /opt/airflow/dags/