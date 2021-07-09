.. image:: https://img.shields.io/badge/github-repo-000.svg?logo=github&labelColor=gray&color=blue
   :target: https://github.com/SecConNet/mahiru
   :alt: GitHub Badge

.. image:: https://github.com/SecConNet/mahiru/workflows/Continuous%20Integration/badge.svg
   :target: https://github.com/SecConNet/mahiru/actions?query=workflow%3A%22Continuous+Integration%22
   :alt: Continuous Integration Badge

.. image:: https://img.shields.io/github/license/SecConNet/mahiru
   :target: https://github.com/SecConNet/mahiru
   :alt: License Badge

#######################################
Mahiru Data Exchange - Proof of Concept
#######################################

A proof of concept for a federated, policy-driven data exchange for the
SecConNet project.

Mahiru is a design for a distributed data processing system which lets data and
software owners share their data and software while keeping it under their
control to any desired extent by defining policies. Users (or the applications
they use) submit workflows expressing their desired data processing operation,
and the system will determine automatically whether the workflow can be
executed, and if so how.

Mahiru's execution model is very flexible, and can support data downloads,
compute-to-data, software-as-a-service, infrastructure-as-a-service, trusted
third parties, and distributed machine learning, and it will automatically
assemble any combination of these required to fulfil a request.

Mahiru is a federated design, which means that participating parties can run a
Mahiru site on their own hardware, on their own premises and fully under their
own control. Their policies will be stored in their own system, and enforced by
it. Users may also allow their data to be copied to other sites, in which case
they will have to trust those sites to apply and enforce their policies on their
behalf. If desired, sites can be run in the cloud or at a trusted hosting
provider as well.

Policies govern where data can go and how it can be processed by the Mahiru
system. This control may be held exclusively by the data owner, or it can be
delegated to someone else completely or partially, for instance by relying on an
external auditor to audit software used to process the data.

The Mahiru design focuses on secure, federated data exchange and processing.
Buying and selling access rights are outside of its scope, although the system
could be extended to incorporate this.

Contributing
************

If you want to contribute to the development of SecConNet Proof of Concept,
have a look at the `contribution guidelines <CONTRIBUTING.rst>`_.

Legal
*****

Copyright 2020,2021 Netherlands eScience Center and University of Amsterdam

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

