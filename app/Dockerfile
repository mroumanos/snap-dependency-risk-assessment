ARG PYTHON_VERSION=3.14

FROM python:${PYTHON_VERSION}-slim

WORKDIR /build

COPY requirements.txt ./

ARG BUILD_DEPS="\
    build-essential \
    gcc \
    libpq-dev \
    postgresql-client \
    curl"

ARG RUN_DEPS="\
    gcc \
    libpq-dev \
    postgresql-client \
    gdal-bin \
    libgdal-dev"

RUN \
    apt-get -y update; \
    apt-get install -y --no-install-recommends \
        ${BUILD_DEPS}; \
    apt-get install -y --no-install-recommends \
        ${RUN_DEPS}; \
    pip install --upgrade pip; \
    pip install -r requirements.txt; \
	apt-get purge -y --auto-remove -o APT::AutoRemove::RecommendsImportant=false \
        ${BUILD_DEPS}; \
    rm -rf /var/lib/apt/lists/*;

WORKDIR /code

COPY ./code ./