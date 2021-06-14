FROM mahiru-base:latest

COPY src/docker/mahiru/site-gunicorn.conf.py /etc/gunicorn.conf.py
