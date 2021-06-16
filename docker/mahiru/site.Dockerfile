FROM mahiru-base:latest
COPY docker/mahiru/site-gunicorn.conf.py /etc/gunicorn.conf.py
