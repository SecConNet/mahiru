FROM ubuntu:20.04
RUN \
    apt-get update && \
    apt-get -y upgrade && \
    apt-get -y install nginx && \
    apt-get -y install python3 python3-pip python3-venv python3-wheel
RUN \
    useradd mahiru && \
    mkdir /home/mahiru && \
    chown -R mahiru:mahiru /home/mahiru
RUN \
    mkdir /var/log/gunicorn && \
    chown mahiru:mahiru /var/log/gunicorn && \
    mkdir /var/run/gunicorn && \
    chown mahiru:mahiru /var/run/gunicorn

COPY docker/nginx.conf /etc/nginx/sites-available/default
COPY docker/init.sh /usr/local/bin/init.sh
RUN chmod +x /usr/local/bin/init.sh

COPY . /home/mahiru/
RUN chown -R mahiru:mahiru /home/mahiru
RUN pip3 install --system gunicorn /home/mahiru

USER root
CMD ["/usr/local/bin/init.sh", "proof_of_concept.rest.registry:wsgi_app()"]
