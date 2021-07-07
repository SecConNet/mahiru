# Logging
accesslog = '-'
errorlog = '-'
capture_output = True

# Listening
bind = ['0.0.0.0:8000']

# App
wsgi_app = 'mahiru.rest.registry:wsgi_app()'
