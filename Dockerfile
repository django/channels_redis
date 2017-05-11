FROM ubuntu:16.04

RUN DEBIAN_FRONTEND=noninteractive apt-get update && \
    apt-get -yqq install \
    build-essential python-pip software-properties-common \
    python-dev python3-dev \
    libffi-dev libxml2-dev libxslt-dev libssl-dev

RUN add-apt-repository ppa:fkrull/deadsnakes && \
    apt-get update

RUN DEBIAN_FRONTEND=noninteractive apt-get -yqq install \
    python2.7 python3.4 python3.5

RUN pip install -U pip && pip install tox

ADD . /src
WORKDIR /src

CMD ["tox"]
