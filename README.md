# The platt ceph gateway #

This program supplies the platt backend with data from a ceph server.

## Requirements ##

For compatibility reasons (rados library) this needs a Python 3.5 environment.
This can be easily set up in anaconda.

## Usage ##

Before running make sure to enable the Python 3.5 environment, e.g. by typing
`source activate py35`.

Calling the program is then done with `./main.py -c $(CEPH_CONFIG) -u
$(CEPH_POOL_USER) -p $(CEPH_POOL)`.

`$(CEPH_CONFIG)` is a config file that looks roughly like this:

```
[global]
        fsid = ...
        mon_initial_members = ...
        mon_host = ...
        auth_cluster_required = ...
        auth_service_required = ...
        auth_client_required = ...
        public network = ...

[client. ...]
        key = ...
        caps mon = ...
        caps osd = ...
```

## Notes ##

A note on readiness time: when the ceph cluster contains many files across many
namespaces the starting time can be upwards of 15 minutes. To assert that things
are actually happening in the background it may be helpful to enable verbose
logging by appending `-l verbose` to the command line input.


## Usage printout ##

Type `./main.py -h` for the help.

```
usage: main.py [-h] -c CONFIG -p POOL -u USER [-b BACKEND_PORT]
               [-s SIMULATION_PORT]
               [-l {debug,verbose,info,warning,error,critical,quiet}] [--test]

Deliver data from the ceph cluster to the platt backend.

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Path to the configuration file (default: None)
  -p POOL, --pool POOL  Name of the ceph pool (default: None)
  -u USER, --user USER  Ceph user name (default: None)
  -b BACKEND_PORT, --backend_port BACKEND_PORT
                        The port on which the backend connects (default: 8009)
  -s SIMULATION_PORT, --simulation_port SIMULATION_PORT
                        The port on which the simulation can connect (default:
                        8010)
  -l {debug,verbose,info,warning,error,critical,quiet}, --log {debug,verbose,info,warning,error,critical,quiet}
                        Set the logging level (default: info)
  --test                Perform unittests and exit afterwards (default: False)
```
