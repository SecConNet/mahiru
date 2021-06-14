FROM python:3.9-slim
RUN \
    useradd mahiru && \
    mkdir /home/mahiru && \
    chown -R mahiru:mahiru /home/mahiru
RUN \
    mkdir /var/run/gunicorn && \
    chown mahiru:mahiru /var/run/gunicorn

COPY docker/mahiru/init.sh /usr/local/bin/init.sh
RUN chmod +x /usr/local/bin/init.sh

COPY setup.py /home/mahiru/setup.py
COPY MANIFEST.in /home/mahiru/MANIFEST.in
COPY LICENSE /home/mahiru/LICENSE
COPY NOTICE /home/mahiru/NOTICE
COPY README.rst /home/mahiru/README.rst
COPY proof_of_concept /home/mahiru/proof_of_concept

RUN chown -R mahiru:mahiru /home/mahiru
RUN pip install gunicorn[gevent] /home/mahiru

EXPOSE 8000
CMD ["/usr/local/bin/init.sh"]
