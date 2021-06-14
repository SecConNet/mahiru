# Logging
accesslog = '/var/log/gunicorn/access.log'
errorlog = '/var/log/gunicorn/error.log'
capture_output = True

# Listening
bind = ['0.0.0.0:8000']

# App
wsgi_app = 'proof_of_concept.rest.registry:wsgi_app()'
