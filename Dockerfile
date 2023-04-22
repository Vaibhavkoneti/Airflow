FROM apache/airflow:2.2.0
RUN pip install --no-cache-dir 'plyvel==1.3.0'
RUN pip install --no-cache-dir 'psycopg2-binary'
RUN pip install --no-cache-dir 'google-cloud-bigquery'
RUN pip install --no-cache-dir 'apache-airflow-providers-google'

