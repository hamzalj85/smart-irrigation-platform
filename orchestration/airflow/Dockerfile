FROM apache/airflow:3.1.8-python3.12

# --- Libraries used by the Python tasks inside the DAGs ----------------------
# duckdb   : validating the marts produced by dbt
# pymongo  : reading the current device state
# paho-mqtt: reading the status topic retained by the broker
RUN pip install --no-cache-dir \
        "duckdb>=1.0,<2" \
        "pymongo>=4.8,<5" \
        "paho-mqtt>=2.1,<3" \
        "scikit-learn>=1.5,<2" \
        "joblib>=1.4,<2" \
        "boto3>=1.34,<2"

# --- dbt in a separate virtual environment -----------------------------------
# dbt and Airflow share dependencies (pydantic, protobuf, jinja2) whose version
# constraints conflict on a regular basis. Isolating them avoids breaking one
# by upgrading the other. The DAGs call the binary by absolute path:
# /home/airflow/dbt-venv/bin/dbt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.9.10" \
        "dbt-duckdb==1.9.6"

ENV DBT_PROFILES_DIR=/opt/airflow/dbt
