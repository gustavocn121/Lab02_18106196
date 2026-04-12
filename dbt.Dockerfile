FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends git

RUN pip install --no-cache-dir dbt-postgres==1.8.2

WORKDIR /dbt

CMD bash -c "\
    dbt debug --profiles-dir /dbt && \
    dbt run --profiles-dir /dbt && \
    dbt test --profiles-dir /dbt"
