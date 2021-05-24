FROM mahiru-base:latest

COPY src/docker/mahiru/site.conf /etc/mahiru/mahiru.conf

CMD ["/usr/local/bin/init.sh", "proof_of_concept.rest.ddm_site:wsgi_app()"]
