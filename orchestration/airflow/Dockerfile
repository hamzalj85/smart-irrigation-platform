FROM apache/airflow:3.1.8-python3.12

# --- Librairies utilisees par les taches Python des DAG -----------------------
# duckdb   : validation des marts produites par dbt
# pymongo  : lecture de l'etat courant des appareils
# paho-mqtt: lecture du topic de statut retenu par le broker
RUN pip install --no-cache-dir \
        "duckdb>=1.0,<2" \
        "pymongo>=4.8,<5" \
        "paho-mqtt>=2.1,<3"

# --- dbt dans un environnement virtuel separe --------------------------------
# dbt et Airflow ont des dependances communes (pydantic, protobuf, jinja2) dont
# les contraintes de version entrent regulierement en conflit. Les isoler evite
# de casser l'un en mettant l'autre a jour. Les DAG appellent le binaire par son
# chemin absolu : /home/airflow/dbt-venv/bin/dbt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir --upgrade pip \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir \
        "dbt-core==1.9.10" \
        "dbt-duckdb==1.9.6"

ENV DBT_PROFILES_DIR=/opt/airflow/dbt
