ARG PYTHON_VERSION=3.10
FROM python:${PYTHON_VERSION}-slim

COPY --from=ghcr.io/astral-sh/uv:0.9.27 /uv /uvx /bin/

WORKDIR /app

COPY . /app

RUN uv sync --locked --extra research

CMD ["uv", "run", "--locked", "--extra", "research", "prediction-market-sports-benchmark-suite", "--output-dir", "runtime/benchmark-suite"]
