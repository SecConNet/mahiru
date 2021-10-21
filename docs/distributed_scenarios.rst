Running distributed scenarios
=============================

Mahiru is a distributed system. While that doesn't stop anyone from running a
complete Mahiru system on a single machine, some experiments require setting up
Mahiru on actual different machines to get realistic results.

So, we need some way of running distributed experiments: we obtain some
(physical or virtual) machines, install a Mahiru registry on one of them and
some sites on others, and run some kind of distributed processing scenario.
Mahiru has a set-up for this, and this page explains how to work with it.

Installing software
-------------------

The distributed set-up uses `Vagrant <https://vagrantup.com>`_ and `Ansible
<https://ansible.com>`_ to orchestrate the experiment, and we'll also need
`Docker <https://docker.com>`_ to build some container images. I'm running this
on Ubuntu Linux, so that's what these instructions are for. If you have a
different OS, then you'll probably have to do some figuring-out to get things to
work.

Vagrant creates virtual machines according to a desired configuration and takes
care of setting up access via SSH. Ansible configures virtual or physical
machines by connecting to them, examining their state, and taking action to
ensure it matches some desired state described in its configuration. Vagrant can
call Ansible to configure (or *provision*) the VMs it has created. Docker is
used both to deploy Mahiru on the machines and by Mahiru itself to execute
workflow steps.

If you use Vagrant, then the experiment will be set up locally on a set of
virtual machines. If you have virtual or physical machines available already,
then you can skip Vagrant and instead tell Ansible about them, then use Ansible
directly to install the software and run the experiment.

I'm running this locally, with Vagrant and Ansible, and using ``libvirt`` with
``KVM/QEMU`` as the hypervisor for my VMs. Vagrant supports a range of
hypervisors, the default being VirtualBox. VirtualBox is easy to install, but it
isn't fully open source and it's considered a bit less stable. All this should
work with VirtualBox though, so feel free to use it.

Installing Vagrant
``````````````````

To install Vagrant, follow the `Vagrant installation instructions
<https://www.vagrantup.com/downloads>`_.

If you are using ``KVM/QEMU`` then you may need to set the environment variable
``VAGRANT_DEFAULT_PROVIDER`` to ``libvirt`` for it to automatically talk to the
correct hypervisor.

Installing and configuring Ansible
``````````````````````````````````

Ansible is written in Python, and can be installed using ``pip``:

.. code-block:: bash

  pip3 install ansible

More information on installing Ansible can be found in the `Ansible Installation
Guide <https://docs.ansible.com/ansible/latest/installation_guide/index.html>`_.


Configuring machines with Ansible can be rather slow, but there are some
settings that can be changed to improve that, and it's well worth doing so. To
do this, create a file in your home directory called `.ansible.cfg` and add the
following content:

.. code-block:: ini

  [defaults]
  use_persistent_connections = True
  ssh_args = -o ControlMaster=auto -o ControlPersist=36000s
  control_path = %(directory)s/ansible-ssh-%%h-%%p-%%r
  pipelining = True

  deprecation_warnings = False


These settings make Ansible connect to the machine that is being configured
once, then reuse the connection to do multiple things. This is quite a bit
faster than reconnecting all the time, because SSH has a lot of connection
set-up overhead.

With my particular combination of Vagrant and Ansible, I was getting some
deprecation warnings. Everything seems to work fine however, so I disabled them
here to cut down on the clutter.

Installing Docker
`````````````````

To install Docker, follow the `Instructions for installing Docker Engine
<https://docs.docker.com/engine/install/>`_ to get the latest version. Your
Linux distribution may have Docker available as well, in which case you could
install it from there. However, it does pay to have a recent version of Docker
with support for Buildkit, because that lets you build images much more quickly.


Building container images
-------------------------

We'll be running the Mahiru registry and Mahiru sites on our machines, and we're
going to use Docker to deploy it. To make that work, we need to build some
Docker images containing Mahiru. This is done using ``make`` in the root of the
repository:

.. code-block:: bash

  make docker_tars


(If you don't have ``make`` installed, this will give an error. ``sudo apt-get
install make`` should fix that.)

This also builds some helper images that are used by Mahiru internally. These
end up in `mahiru/data`, from where they are included into the Mahiru Docker
image, and then Mahiru will load them into Docker after it starts up.

Mahiru runs workflows, and each step in a workflow executes a program or script.
To allow sending these programs to other sites and running them there, they are
packaged up into container images as well. The same goes for data sets. Mahiru
calls these programs and data sets *assets*. To run our experiment, we'll need
some of these asset container images. They can be built in the same way:

.. code-block:: bash

  make assets


The container images this produces can be found in the `build/` directory, if
you're interested (but honestly, there's not much to see).


Running an experiment
---------------------

There's currently a single distributed experiment that comes with Mahiru, a
simple compute-to-data scenario. We'll first run it, and then explain how it
works and how it is implemented. Note that this will take about 2.5 GB of RAM
and about 11 GB of disk space.

The compute to data scenario is in ``scenarios/compute_to_data``, and can be
launched using Vagrant:

.. code-block:: bash

  cd scenarios/compute_to_data
  vagrant up


If everything is configured correctly, this will:

- Create five virtual machines using the default hypervisor,
- name the machines ``registry``, ``site1``, ``site2``, ``client1``, and
  ``client2``,
- install Docker on the ``registry`` and ``site*`` machines,
- install Mahiru on all machines,
- configure the sites,
- and start the registry and the sites.

This gives us a working data exchange with two sites, both of which are
registered with the registry and aware of each other's existence. For each site,
there is also a client machine, which represents a computer used by an
administrator or a user of the system.

In order to run our experiment, the sites need to be configured by an
administrator. Assets need to be uploaded, and policies need to be set to give
permission to do things with them. This is done using the ``vagrant provision``
command:

.. code-block:: bash

  vagrant provision --provision-with configure_sites client1 client2

Here, we specify a that a specific provisioning step should be run, namely
``configure_sites``, and that it should be run on machines ``client1`` and
``client2``. This runs a site-specific script on each client machine which
connects to the corresponding site and sets it up to be able to run a simple
compute-to-data scenario.

Finally, we can submit a processing workflow to ``site1`` via ``client1``. Due
to the way the policies are set up, it will be executed in a compute-to-data
way:

.. code-block:: bash

  vagrant provision --provision-with run_experiment client1


The typical workflow after making changes to Mahiru or to the experiment is to
first shut down Mahiru, then restart it with the new code, and then repeat
``configure_sites`` and ``run_experiment`` as above. Stopping and starting
Mahiru without destroying and recreating the VMs can be done with

.. code-block:: bash

  vagrant provision --provision-with mahiru_down registry site1 site2
  vagrant provision --provision-with mahiru_up registry site1 site2 client1 client2

Note the addition of `client1` and `client2` for the `mahiru_up` run. This will
install the new version on the clients as well for use by the control scripts.

To destroy the VMs, use (without -f you'll have to confirm the destruction of
each individual VM):

.. code-block:: bash

  vagrant destroy -f


How it works
------------

This section is a bit short for now, but hopefully provides a bit of a start to
figuring out how all this is put together.


Vagrant and Ansible
```````````````````

Vagrant gets its configuration from ``scenarios/compute_to_data/Vagrantfile``.
This in turn refers to Ansible configuration files in ``scenarios/common`` and
``scenarios/compute_to_data``. Here is what they do:

scenarios/common/base.yml
  Installs Docker and Python 3 and ensures sites can find each other on the
  network. Nothing Mahiru-specific is done yet.

scenarios/common/mahiru_up.yml
  Installs Mahiru Docker containers on ``registry`` and ``site*``, installs an
  nginx-based reverse proxy, and starts the containers. This also installs
  Mahiru as a Python module on the clients for use by scripts. This results in a
  running distributed data exchange system with no data or policies in it.

scenarios/common/mahiru_down.yml
  Stops the Docker containers on ``registry`` and ``site*``. Since Mahiru
  doesn't currently have persistent storage, this deletes any assets and
  policies. After this, you can run ``mahiru_up.yml`` again to do a fresh start
  of the data exchange system.

scenarios/compute_to_data/configure_sites.yml
  Uploads asset containers to the clients, installs set-up and experiment run
  scripts on the clients, and then calls the set-up scripts. These upload assets
  to the corresponding sites, and add policies. After this, the sites are ready
  to run jobs.

scenarios/compute_to_data/run_experiment.yml
  Runs the experiment run script, which will connect to the corresponding site
  and submit a job.


Registry and Sites
``````````````````

Mahiru uses REST APIs to communicate between the distributed components. It's
written in Python 3, and the Docker containers for the registry and site contain
a Gunicorn server with Mahiru running as a WSGI app. On the corresponding VMs,
we run Gunicorn behind an nginx-based reverse proxy (or two for the site, see
below), as recommended in the Gunicorn documentation. This reverse proxy is in
its own Docker container, so there are two Docker containers on the registry VM,
and three each in each site VM.

Sites have two APIs, an internal one for use by clients, and an external one to
communicate with other sites. WSGI apps only have one endpoint, but we can solve
that by running two reverse proxies each forwarding to a different prefix on the
internal API. All this is set up in ``scenarios/common/mahiru_up.yml``, with the
nginx configurations in ``scenarios/common/registry/`` and
``scenarios/common/site/`` respectively.


Clients
```````

Clients are fairly simple, it's just a VM with Python3 installed, and Mahiru
installed as a module so that the scripts can use it to connect to the site and
do their thing. The scripts are in ``scenarios/compute_to_data/client1`` and
``scenarios/compute_to_data/client2``.


Solving problems
----------------

The above should work out of the box, but if it doesn't, or if you've changed
something and it doesn't work right away, you'll want to try to figure out what
happened. You will probably get an error message, but often you will need to
inspect the state of the various VMs and containers.

To connect to a running VM that was started by Vagrant, you can do:

.. code-block:: bash

  vagrant ssh <machine>


Once logged in, it's often useful to check if all the Docker containers are
running, or if they crashed:

.. code-block:: bash

  sudo docker ps -a


The containers are all configured to log to standard output, which is captured
by Docker and can be accessed via:

.. code-block:: bash

  sudo docker logs <container>


Finally, configuration files and such are uploaded into ``~/mahiru`` so you may
want to have a look around there as well.

So for example, if you get an error message from the ``configure_sites``
provisioner saying that ``client1`` cannot connect, then you'll want to take a
look at the corresponding site because it's probably not running:

.. code-block:: bash

  vagrant ssh site1


Maybe ``sudo docker ps -a`` shows you that the Mahiru container has crashed, in
which case

.. code-block:: bash

  sudo docker logs mahiru-site


will probably show you a Python backtrace, from where you can hopefully solve
the problem.

After changing something, it's not always necessary to do a ``mahiru_down``, but
it's a good idea since that resets everything to a known state. Then you can do
``mahiru_up`` again to try if the fix worked.

