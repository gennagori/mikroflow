FROM python:3.11-slim
WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
COPY db ./db
RUN pip install --no-cache-dir .
# entrypoint chosen per-service in docker-compose
CMD ["python", "-m", "mikroflow.collector.main"]
