FROM apache/airflow:3.1.6-python3.12

USER airflow

RUN pip install --no-cache-dir \
    duckdb==1.4.4 
    
RUN pip install --no-cache-dir apache-airflow-sdk

USER airflow