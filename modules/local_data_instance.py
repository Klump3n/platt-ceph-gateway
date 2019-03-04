# #!/usr/bin/env python3
# """
# Maintain a local copy of the data from the ceph cluster.

# """
# from modules.loggers import (CoreLog as cl,
#                              BackendLog as bl,
#                              SimulationLog as sl)

# class DataCopy(object):
#     """
#     A local copy of the data on the ceph cluster.

#     """
#     _instance = None
#     _hashset = set()
#     _local_copy = dict()

#     def __new__(cls):
#         if not cls._instance:
#             cls._instance = cls
#             # cls.__init__()
#         return cls._instance

#     @classmethod
#     def _reset(cls):
#         """
#         Delete the instance.

#         """
#         cls._instance = None
#         cls._hashset = set()
#         cls._local_copy = dict()
#         del cls

#     @classmethod
#     def name_is_present(cls, namespace, name):
#         """
#         Check if a name is already present in the local copy.

#         """
#         hashed_name = hash(str("{}\t{}".format(namespace, name)))
#         if hashed_name in cls._hashset:
#             return True
#         else:
#             return False

#     @classmethod
#     def add_file(cls, namespace, key, sha1sum):
#         sl.debug("Adding file {}/{}/{}".format(namespace, key, sha1sum))
#         hashed_key = hash(str("{}\t{}".format(namespace, key)))

#         if hashed_key not in cls._hashset:

#             try:
#                 # namespace_split_key = namespace_key.split("\t")
#                 # namespace = namespace_split_key[0]
#                 # key = namespace_split_key[1]
#                 string = key.split("universe.fo.")[1]
#                 field, timestep = string.split("@")
#                 usage_rest = field.split(".")
#                 usage = usage_rest[0]
#                 if usage == "elements":
#                     elemtype = usage_rest[1]
#                     fieldname = None
#                 elif usage == "nodes":
#                     elemtype = None
#                     fieldname = None
#                 elif usage == "elemental":
#                     elemtype = usage_rest[1]
#                     fieldname = "_".join(str(r) for r in usage_rest[2:])
#                 elif usage == "nodal":
#                     elemtype = None
#                     fieldname = "_".join(str(r) for r in usage_rest[1:])
#                 elif usage == "elset":
#                     elemtype = usage_rest[1]
#                     fieldname = ".".join(str(r) for r in usage_rest[2:])
#                 elif usage == "skin":
#                     elemtype = usage_rest[1]
#                     fieldname = None
#                 else:
#                     sl.debug("Can not add file {}/{}".format(namespace, key))
#                     return
#             except:
#                 # YOU SHALL NOT PARSE
#                 sl.debug("Can not add file {}/{}".format(namespace, key))
#                 return

#             cls._hashset.add(hashed_key)

#             # Create the dictionary
#             if namespace not in cls._local_copy:
#                 cls._local_copy[namespace] = {}

#             if timestep not in cls._local_copy[namespace]:
#                 cls._local_copy[namespace][timestep] = {}

#             if usage not in cls._local_copy[namespace][timestep]:
#                 cls._local_copy[namespace][timestep][usage] = {}

#             if usage == "nodes":
#                 i_entry = cls._local_copy[namespace][timestep][usage]

#             # if usage == "elements":
#             if (usage in ["skin", "elements"]):
#             # if (usage in ["elset", "elements"]):

#                 if elemtype not in cls._local_copy[namespace][timestep][usage]:
#                     cls._local_copy[namespace][timestep][usage][elemtype] = {}
#                 i_entry = cls._local_copy[namespace][timestep][usage][elemtype]

#             if usage in ["elemental", "elset"]:
#             # if usage == "elemental":
#                 if fieldname not in cls._local_copy[namespace][timestep][usage]:
#                     cls._local_copy[namespace][timestep][usage][fieldname] = {}
#                 if elemtype not in cls._local_copy[namespace][timestep][usage][fieldname]:
#                     cls._local_copy[namespace][timestep][usage][fieldname][elemtype] = {}
#                 i_entry = cls._local_copy[namespace][timestep][usage][fieldname][elemtype]

#             if usage == "nodal":
#                 if fieldname not in cls._local_copy[namespace][timestep][usage]:
#                     cls._local_copy[namespace][timestep][usage][fieldname] = {}
#                 i_entry = cls._local_copy[namespace][timestep][usage][fieldname]

#             i_entry['object_key'] = key
#             # i_entry['object_key'] = namespace_key.split("\t")[1]
#             i_entry['sha1sum'] = sha1sum

#     @classmethod
#     def get_index(cls, namespace=None):
#         if namespace:
#             try:
#                 return cls._local_copy[namespace]
#             except KeyError:
#                 return None
#         return cls._local_copy
