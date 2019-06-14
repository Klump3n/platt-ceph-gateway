#!/USSR/bin/env python3
"""
Opens a steady connection to the ceph cluster.

It tries to read tasks from a queue and returns the results.

"""
import queue
import pathlib
import asyncio
import hashlib
import subprocess
import multiprocessing

try:
    import rados.rados as rados   # comes from python3-rados_12.2.7-1_bpo90+1_amd64.deb
except ImportError:
    print("\n\nThis module needs a working python3.5 environment!\n\n")
    raise

from util.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl


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


class CephConnection(object):
    """
    Maintain a steady connection to the ceph cluster.

    """
    def __init__(
            self,
            ceph_config,
            ceph_pool,
            pool_user,
            queue_ceph_task,    # queue for receiving things to do
            event_shutdown_process,  # when this event is set the connection will be closed
            queue_index,             # return queue for the index
            queue_namespace_index,   # return queue for the index for a namespace
            queue_object_tags,       # return queue for object tags
            queue_object_data,       # return queue for object data (with tags)
            queue_object_hash        # return queue for object hash
    ):
        """
        initialize connection.

        """
        self._conffile = str(pathlib.Path(ceph_config))
        self._target_pool = ceph_pool
        self._rados_id = pool_user

        self._queue_ceph_task = queue_ceph_task
        self._event_shutdown_process = event_shutdown_process

        self._queue_index = queue_index
        self._queue_namespace_index = queue_namespace_index
        self._queue_object_tags = queue_object_tags
        self._queue_object_data = queue_object_data
        self._queue_object_hash = queue_object_hash

        # Connect to cluster
        self._cluster = rados.Rados(
            conffile=self._conffile,
            rados_id=self._rados_id
        )

        self._cluster.connect()

        # Try opening an IO context
        try:
            self._ioctx = self._cluster.open_ioctx(self._target_pool)
        except Exception as ex:
            cl.error("Exception occured: {}".format(ex))
            raise

        try:
            #
            # asyncio: watch the queue and the shutdown event
            self._loop = asyncio.get_event_loop()

            # task for reading the queue
            self._queue_reader_task = self._loop.create_task(
                self._queue_reader_coro())

            self._loop.run_until_complete(self._queue_reader_task)

            # stop the event loop
            self._loop.call_soon_threadsafe(self._loop.stop())

            self.__del__()
            cl.debug("Shutdown of ceph_connection process complete")

        except KeyboardInterrupt:
            # Ctrl C passes quietly
            pass

    async def _queue_reader_coro(self):
        """
        Read the queue for new things to do.

        """
        while True:
            new_task = await self._loop.run_in_executor(
                None, self._queue_reader_executor)

            if not new_task:
                # return None when we want to stop
                return None

            try:
                task = new_task["task"]
                task_info = new_task["task_info"]

            except KeyError:
                cl.warning("Could not read task dictionary {}".format(new_task))

            else:

                if (task == "read_index"):
                    cl.debug("Reading index, task_info = {}".format(task_info))
                    index_dict = self.read_index(task_info)
                    self._queue_index.put(index_dict)

                if (task == "read_namespace_index"):
                    cl.debug("Reading namespace index, task_info = {}".format(task_info))
                    namespace_index_dict = self.read_index_for_namespace(task_info)
                    self._queue_namespace_index.put(namespace_index_dict)

                if (task == "read_object_value"):
                    cl.debug("Reading object value, task_info = {}".format(task_info))
                    object_value_dict = self.read_everything_for_object(task_info)
                    self._queue_object_data.put(object_value_dict)

                if (task == "read_object_tags"):
                    cl.debug("Reading object tags, task_info = {}".format(task_info))
                    object_value_dict = self.read_tags_for_object(task_info)
                    self._queue_object_tags.put(object_value_dict)

                if (task == "read_object_hash"):
                    cl.debug("Reading object hash, task_info = {}".format(task_info))
                    object_value_dict = self.read_hash_for_object(task_info)
                    self._queue_object_hash.put(object_value_dict)

    def _queue_reader_executor(self):
        """
        Read the queue in a separate executor.

        """
        while True:

            if self._event_shutdown_process.is_set():
                cl.debug("Ceph connection shutdown event is set")
                return None

            try:
                new_ceph_task = self._queue_ceph_task.get(True, .1)
            except queue.Empty:
                pass
            else:
                return new_ceph_task

    def __del__(self):
        """
        Close and shutdown the connection.

        """
        try:
            self._ioctx.close()
            cl.debug("Ceph IO context closed")
        except:
            cl.debug("Could not close ceph IO context")

        try:
            self._cluster.shutdown()
            cl.debug("Cluster access shut down")
        except:
            cl.debug("Could not shutdown cluster access")

    def _set_namespace(self, namespace):
        """
        Set the namespace.

        """
        self._ioctx.set_namespace(namespace)

    def _unset_namespace(self):
        """
        Unset the namespace.

        """
        self._ioctx.set_namespace("")

    def _get_index(self):
        """
        Read all the object names and tags.

        """
        index = dict()

        obj_iter = self._ioctx.list_objects()

        for rados_obj in obj_iter:
            obj_name = rados_obj.key

            index[obj_name] = {}
            obj_dict = index[obj_name]

            obj_xattrs = self._ioctx.get_xattrs(obj_name)
            for xattr_key, xattr_val in obj_xattrs:
                obj_dict[xattr_key] = xattr_val.decode()

        return index

    def _get_objval(self, objname):
        """
        Get the value of an object.

        """
        obj_size = self._ioctx.stat(objname)[0]
        objval = self._ioctx.read(objname, length=obj_size)

        return objval

    def _get_objtags(self, objname):
        """
        Get the tags for an object.

        """
        tags_dict = {}

        obj_xattrs = self._ioctx.get_xattrs(objname)
        for xattr_key, xattr_val in obj_xattrs:
            tags_dict[xattr_key] = xattr_val.decode()

        # if the sha1sum is not calculated we generate it
        try:
            sha1sum = tags_dict["sha1sum"]
        except KeyError:
            sha1sum = self._calc_and_write_objhash(objname)
        else:
            if sha1sum == "":
                sha1sum = self._calc_and_write_objhash(objname)

        tags_dict["sha1sum"] = sha1sum

        return tags_dict

    def _calc_and_write_objhash(self, objname):
        """
        Calculate the objhash and write it to the obj tags on the cluster.

        """
        cl.debug("Calculating hash for {}".format(objname))
        objval = self._get_objval(objname)
        objhash = hashlib.sha1(objval).hexdigest()

        try:
            self._ioctx.set_xattr(objname, "sha1sum", objhash.encode())
        except AttributeError:  # can't encode objhash
            pass

        return objhash

    def __remove_objhash(self, objname):
        """
        Remove the sha1sum for an object.

        """
        self._ioctx.rm_xattr(objname, "sha1sum")

    def read_index(self, task_info):
        """
        Return the complete index.

        """
        namespaces = get_namespaces(
            self._conffile, self._target_pool, self._rados_id)
        expected_namespaces = namespaces.copy()

        for namespace in namespaces:
            task_dict = {
                "task": "read_namespace_index",
                "task_info": {
                    "namespace": namespace
                }
            }
            self._queue_ceph_task.put(task_dict)

        index = list()

        while not len(expected_namespaces) == 0:
            namespace_index = self._queue_namespace_index.get()
            index.append(namespace_index)
            index_name = namespace_index["namespace"]
            expected_namespaces.remove(index_name)

        return {"index": index}

    def read_index_for_namespace(self, task_info):
        """
        Generate the index for a namespace.

        Returns a list of dictionaries with object attributes.

        """
        namespace = task_info["namespace"]

        self._set_namespace(namespace)

        index = self._get_index()

        return_dict = dict()
        return_dict["namespace"] = namespace
        return_dict["index"] = index

        self._unset_namespace()

        return return_dict

    def read_everything_for_object(self, task_info):
        """
        Get the file itself and all the tags.

        """
        namespace = task_info["namespace"]
        obj_name = task_info["object"]

        self._set_namespace(namespace)

        objval = self._get_objval(obj_name)

        tags_dict = self._get_objtags(obj_name)

        self._unset_namespace()

        return_dict = dict()
        return_dict["namespace"] = namespace
        return_dict["object"] = obj_name
        return_dict["tags"] = tags_dict
        return_dict["value"] = objval

        return return_dict

    def read_tags_for_object(self, task_info):
        """
        Get the tags for an object.

        This can be used to update the local data copy.

        """
        namespace = task_info["namespace"]
        obj_name = task_info["object"]

        self._set_namespace(namespace)

        tags_dict = self._get_objtags(obj_name)

        self._unset_namespace()

        return_dict = dict()
        return_dict["namespace"] = namespace
        return_dict["object"] = obj_name
        return_dict["tags"] = tags_dict

        return return_dict

    def read_hash_for_object(self, task_info):
        """
        This reads the sha1sum from the ceph cluster.

        If no sha1sum can be found it will be calculated by downloading and
        hashing the file. The result will be written to the cluster and then
        returned.

        """
        namespace = task_info["namespace"]
        obj_name = task_info["object"]

        self._set_namespace(namespace)

        tags_dict = self._get_objtags(obj_name)

        self._unset_namespace()

        # tags_dict["sha1sum"] = sha1sum

        return_dict = dict()
        return_dict["namespace"] = namespace
        return_dict["object"] = obj_name
        return_dict["tags"] = tags_dict

        return return_dict
