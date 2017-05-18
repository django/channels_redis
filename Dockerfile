FROM ubuntu:16.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get -yqq install \
    build-essential python-pip software-properties-common \
    python-dev python3-dev \
    libffi-dev libxml2-dev libxslt-dev libssl-dev

RUN pip install -U pip && pip install tox

ENV DOCKER_TEST_ENV true

ADD . /src
WORKDIR /src

CMD ["tox"]
