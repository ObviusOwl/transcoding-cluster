FROM ubuntu:18.04

RUN apt-get update && apt-get install -y --no-install-recommends\
        python3 \
        python3-venv \
        python3-yaml \
        python3-mysql.connector \
        python3-requests \
        python3-pip \
        python3-jinja2 \
        python3-lxml \
        ffmpeg \
        libavcodec-extra \
        uwsgi \
        uwsgi-plugin-python3 \
    && apt-get clean -y && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*


RUN adduser --disabled-password --gecos '' jojo
EXPOSE 5000/tcp
VOLUME /data

RUN mkdir -p /opt/tc-venv \
    && python3 -m venv /opt/tc-venv --system-site-packages \
    && /opt/tc-venv/bin/pip3 install Flask

COPY ./docker/run.sh /run.sh

ENV FLASK_ENV=development
ENV TRANSCODING_CLUSTER_CONFIG=transcoding-cluster.ini

COPY . /opt/tc

WORKDIR /data
USER 1000
ENTRYPOINT ["/run.sh"]
