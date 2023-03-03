FROM python:3.8-slim-buster

RUN apt-get update && apt-get install -y \
    build-essential \
    gcc

WORKDIR /app

COPY . .

# Install dependencies
RUN pip install .

CMD ["tail", "-f", "/dev/null"]
