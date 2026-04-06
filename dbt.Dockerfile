FROM python:3.11-slim

RUN pip install --no-cache-dir dbt-postgres==1.8.2

WORKDIR /dbt

ENTRYPOINT ["bash", "-c"]
CMD ["dbt run --profiles-dir /dbt"]
