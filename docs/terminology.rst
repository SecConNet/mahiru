===========
Terminology
===========

This proof-of-concept contains quite a few classes which play different roles in
the system. They're named systematically, so that we have some hope of being
able to see what is what. Here's a key to class names:

...Service
  A service is a class which offers a service to other sites. Its interface is
  in Python, this is not a REST service or network service.

...Client
  Manages a local replica of a remote canonical store, like the registry or the
  policy stores at other sites.

...Handler
  A class which handles a specific type of REST request.

...RestApi
  Represents a REST API. Contains Handlers, can be served using a Server, and
  contains an App object which can be used when running in a UWSGI server.

...Server
  A simple HTTP server for serving Apis in the test environment.

...RestClient
  Used to connect to a Server serving a RestApi.
