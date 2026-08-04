FROM apache/airflow:2.10.4
USER root
RUN apt-get update \

&& apt-get install -y --no-install-recommends \
build-essential \
&& apt-get autoremove -yqq --purge \
&& apt-get clean \
&& rm -rf /var/lib/apt/lists/*

USER airflow

RUN pip install --no-cache-dir \

astronomer-cosmos==1.6.0 \
apache-airflow-providers-mysql \
apache-airflow-providers-postgres \
apache-airflow-providers-snowflake \
apache-airflow-providers-amazon \
dbt-core==1.7.0 \
dbt-mysql==1.7.0 \
dbt-postgres==1.7.0  
