FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN python -m pip install --upgrade pip \
 && python -m pip install -e ".[research]"

CMD ["prediction-market-sports-benchmark-suite", "--output-dir", "runtime/benchmark-suite"]
