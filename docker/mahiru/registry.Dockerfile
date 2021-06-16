FROM mahiru-base:latest
COPY docker/mahiru/registry-gunicorn.conf.py /etc/gunicorn.conf.py
