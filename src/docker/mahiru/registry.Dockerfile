FROM mahiru-base:latest
CMD ["/usr/local/bin/init.sh", "proof_of_concept.rest.registry:wsgi_app()"]
EXPOSE 8000
