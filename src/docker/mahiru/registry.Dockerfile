FROM mahiru-base:latest
COPY src/docker/mahiru/registry-gunicorn.conf.py /etc/gunicorn.conf.py
