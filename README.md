# The platt ceph gateway #

This program supplies the platt backend with data from a ceph server.


## Requirements ##

For compatibility reasons (rados library) this needs a Python 3.5 environment.
This can be easily set up in anaconda: `conda create -n py35 python=3.5`.


## Usage ##

Before running make sure to enable the Python 3.5 environment, e.g. by typing
`source activate py35`.

Calling the program is then done with `./gateway.py -c $(CEPH_CONFIG) -u
$(CEPH_POOL_USER) -p $(CEPH_POOL_NAME)`.

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


## Connecting the platt-backend ##

The [platt backend](https://github.com/Klump3n/platt-backend) connects by
connecting to the port specified by the `-b BACKEND_PORT` argument (defaults to
8009): `./platt.py -e --ext_address $(GATEWAY_IP) --ext_port $(GATEWAY_PORT)`.


## Adding data to a running gateway ##

Once the gateway has collected information about the simulation data that is
present on the server it relies on the user to feed information into the
gateway. This means that the ceph server is indexed once when starting the
program. Afterwards the gateway must be notified about every new file that is
being uploaded. To do this, send a string with the following format to the
gateway-ip on the port specified by the `-s SIMULATION_PORT` argument (defaults
to 8010): `$(SIMULATION_NAMESPACE)\t$(FILE_NAME)\t$(FILE_SHA1_SUM)`. The three
fields in the string are separated by two tabs. The `$(FILE_SHA1_SUM)` is
optional but can speed up processing.


## Notes ##

A note on readiness time: when the ceph cluster contains many files across many
namespaces the starting time can be upwards of 15 minutes. To assert that things
are actually happening in the background it may be helpful to enable verbose
logging by appending `-l verbose` to the command line input.


## Usage printout ##

Type `./gateway.py -h` for the help.

```
usage: gateway.py [-h] -c CONFIG -p POOL -u USER [-b BACKEND_PORT]
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
