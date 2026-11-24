FROM python:3.12-slim

WORKDIR /app
COPY pyproject.toml ./
COPY src ./src
RUN pip install --no-cache-dir ".[producer]" && useradd -m app
USER app

ENTRYPOINT []
CMD ["python", "-m", "flightstream.producer", "--sink", "eventhub", "--interval", "60"]
