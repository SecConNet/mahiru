# Base image for Mahiru compute assets.

# Compute assets in Mahiru are container images containing software that
# processes data, obtained from connected data assets. This builds a base
# image for Python-based compute assets. The Python script should be put in
# /home/mahiru/app.py, and run as the non-root mahiru user.

FROM ubuntu:latest

RUN \
    apt-get update && \
    apt-get -y dist-upgrade && \
    apt-get -y install python3 python3-pip python3-venv python3-wheel && \
    apt-get clean && rm -rf /var/lib/apt/lists/* /tmp/* /var/tmp/*

RUN \
    useradd mahiru && \
    mkdir /home/mahiru && chown mahiru:mahiru /home/mahiru && \
    mkdir -p /srv/mahiru && chown mahiru:mahiru /srv/mahiru && \
    mkdir /etc/mahiru

USER mahiru

# One day this will be a proper pip-installable library, but for now we'll
# just put it here.
COPY libmahiru.py /home/mahiru/libmahiru.py

CMD ["/home/mahiru/app.py"]
