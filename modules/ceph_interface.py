#!/usr/bin/env python3
"""
Everything related to getting information from the ceph cluster.

"""
import pathlib
import subprocess
from itertools import repeat
import multiprocessing

try:
    import rados.rados as rados   # comes from python3-rados_12.2.7-1_bpo90+1_amd64.deb
except ImportError:
    print("\n\nThis module needs a working python3.5 environment!\n\n")
    raise

from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


def get_namespaces(ceph_conf, ceph_pool, ceph_user):
    """
    Use rados on the commandline to parse all namespaces.

    """
    ceph_conf = str(pathlib.Path(ceph_conf))

    # a set can not have duplicates
    namespaces = set()

    cwd = pathlib.Path(__file__).parent.parent
    pathlib.Path(cwd / ".radosoutput").mkdir(exist_ok=True)
    rados_file = cwd / ".radosoutput" / "radosoutput.txt"

    # delete existing file first
    try:
        rados_file.unlink()
    except FileNotFoundError:
        pass

    try:
        rados_cmd = [
            "rados",
            "-p", "{}".format(ceph_pool),
            "ls", "{}".format(str(rados_file)),
            "--user", "{}".format(ceph_user),
            "--keyring", "{}".format(ceph_conf),
            "--all"
        ]
        result = subprocess.call(rados_cmd)
    except FileNotFoundError:
        cl.warning("call to commandline failed -- file not found")
        return None

    if not result == 0:
        cl.warning("call to command line failed -- non zero exit code")
        return None

    if not rados_file.exists():
        cl.warning("no data for parsing present")
        return None

    cl.debug("parsing data in .radosoutput/radosoutput.txt")

    with open(str(rados_file), 'r') as rf:
        while True:
            line = rf.readline()
            if line == "":
                break
            pts = line.split("\t")  # split on tab
            # len = 2 -> this has a namespace
            if len(pts) == 2:
                if pts[0] != "":
                    namespaces.add(pts[0])

    cl.debug("got {} namespaces".format(len(namespaces)))

    return namespaces


def namespace_index(ceph_config, ceph_pool, pool_user, namespace=None):
    """
    Read metadata from the cluster.

    """
    if not namespace:
        cl.debug("no namespace specified")
        return
    cluster = _CephConnect(ceph_config, ceph_pool, pool_user, namespace)

    return cluster.namespace_index()

def _split_list(lst, n):
    """
    Create a list of lists from a full list.

    """
    return [lst[i::n] for i in range(n)]

class _CephConnect:
    """
    Connect to the ceph cluster and perform the operations.

    """
    def __init__(self, ceph_config, ceph_pool, pool_user, namespace=None):
        """
        Parse the target dictionary.

        """
        self._conffile = str(pathlib.Path(ceph_config))
        self._target_pool = ceph_pool
        self._rados_id = pool_user

        # Connect to cluster
        self._cluster = rados.Rados(
            conffile=self._conffile,
            rados_id=self._rados_id
        )

        self._cluster.connect()

        # Try opening an IO context
        try:
            self._ioctx = self._cluster.open_ioctx(self._target_pool)
            if namespace:
                self._ioctx.set_namespace(namespace)
        except Exception as ex:
            cl.error("Exception occured: {}".format(ex))

    def __del__(self):
        try:
            self._ioctx.close()
        except:
            pass

        try:
            self._cluster.shutdown()
        except:
            pass


    def namespace_index(self):
        """
        Read the metadata information from the server and return it in a
        dictionary.

        """
        return_dict = {}

        obj_iter = self._ioctx.list_objects()

        for rados_obj in obj_iter:
            obj_name = rados_obj.key

            return_dict[obj_name] = {}
            obj_dict = return_dict[obj_name]

            obj_xattrs = self._ioctx.get_xattrs(obj_name)
            for xattr_key, xattr_val in obj_xattrs:
                obj_dict[xattr_key] = xattr_val.decode()

        return return_dict

    # def read_files(self, object_list):
    #     """
    #     Read the list of objects from the cluster and store them in the
    #     directory 'download'.

    #     """
    #     for obj in object_list:
    #         try:
    #             # save without slashes
    #             new_key = obj.replace('/', '_')

    #             obj_size = self._ioctx.stat(obj)[0]
    #             objval = self._ioctx.read(obj, length=obj_size)

    #             with open('download/{}'.format(new_key), 'wb') as write_file:
    #                 write_file.write(objval)

    #         except Exception as ex:
    #             print(ex)
