===========
Terminology
===========

Naming objects in and around the DDM
====================================

There are different kinds of objects in the DDM which need to be distinguished
from other objects of the same kind. So they need some kind of unique
identifier.

Identifiers in the DDM are strings consisting of colon-separated segments. The
first segment is a fixed string designating the type of object being
identified. One or more segments follow depending on the type (see below).
Segments may contain lower- and uppercase letters, digits, underscores,
dashes, and periods.

Here are some of those kinds of objects and how they are identified.

Party
  A party is a real-world entity which takes part in the DDM, usually an
  organization. Parties may own or administer sites. Parties are uniquely
  identified using an identifier of the form::

    party:<namespace>:<name>

  The ``<namespace>`` segment is expected to be a DNS (sub-)domain, while
  ``<name>`` can be any valid segment.

Party Collection
  A party collection is a group of parties. It is used to organize parties and
  make it easier to define rules that apply to multiple parties. Party
  collections are identified using an identifier of the form::

    party_collection:<namespace>:<name>

  The ``<namespace>`` segment is expected to be a DNS (sub-)domain, while
  ``<name>`` can be any valid segment.

Site
  A site is a running software installation which takes part in the DDM. Sites
  have an owner (a Party) and an administrator (also a Party). Sites are
  uniquely identified using an identifier of the form::

    site:<namespace>:<name>

  The ``<namespace>`` segment is expected to be a DNS (sub-)domain, while
  ``<name>`` can be any valid segment.

Asset
  An asset is either a data set or a software segment. Assets are identified
  using an Identifier of the form::

    asset:<namespace>:<name>:<site_namespace>:<site_name>

  The ``<namespace>`` and ``<name>`` segments are as above.
  ``<site_namespace>`` and ``<site_name>`` refer to the site from which the
  asset is available, ``site:<site_namespace>:<site_name>``.

AssetCollection
  An asset collection is a group of assets. It is used to organize assets and
  make it easier to define rules that apply to multiple assets. Asset
  collections are identified using an identifier of the form::

    asset_collection:<namespace>:<name>

  The ``<namespace>`` segment is expected to be a DNS (sub-)domain, while
  ``<name>`` can be any valid segment.

Result
  A result is a data set that is an intermediate or final result of processing
  data using a workflow. Results are identified by an identifier of the form::

    result:<id_hash>

  The ``<id_hash>`` is a string of letters and digits calculated from (the part
  of) the workflow that was used to produce the result.

Workflow item
  A workflow item is a workflow input, workflow output, workflow step, workflow
  step input, workflow step output base, or workflow step output. In the
  context of dealing with permissions for and execution of workflows, it's
  needed to refer to these.  This is done using a string that is not an
  identifier as described above, but a simple string of one of the forms::

    <workflow_input_name>
    <workflow_output_name>
    <step_name>
    <step_name>.<input_name>
    <step_name>.@<output_name>
    <step_name>.<output_name>

  Steps, inputs and outputs are identified by their name, which is a simple
  string consisting of lower- and uppercase letters, digits, and underscores.
  Note that the Workflow class forbids duplicate names among workflow inputs,
  workflow outputs, and steps, so that these are unique within the context of a
  specific workflow. Output base items have an at-sign prepended to the output
  name to distinguish them from the actual output.

Orchestrator
  The orchestrator is a component at a site which receives workflows from users,
  and which plans where to execute each step of the workflow, then coordinates
  those sites on the actual execution.


Class names
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
  A simple HTTP server for serving APIs in the test environment.

...RestClient
  Used to connect to a Server serving a RestApi.
