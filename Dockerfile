FROM python:3.14-slim

WORKDIR /app

COPY pyproject.toml .
COPY uv.lock .

RUN pip install --upgrade pip
RUN pip install uv
RUN uv sync

COPY .env .env
COPY src/ ./src/

ENV PYTHONPATH=/app

CMD ["uv", "run", "python", "-m", "src.main"]
