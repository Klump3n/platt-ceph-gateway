#!/usr/bin/env python3
"""
Test local_data_instance

"""
import unittest

try:
    from modules.local_data_instance import DataCopy
except ImportError:
    import sys
    sys.path.append('..')
    from modules.local_data_instance import DataCopy

from modules.loggers import CoreLog as cl, BackendLog as bl, SimulationLog as sl

class Test_Local_Data_Instance(unittest.TestCase):
    def setUp(self):
        self.dc = DataCopy()

    def tearDown(self):
        self.dc._reset()
        del self.dc

    def test_instance(self):
        """local_data_instance is singleton

        """
        b = DataCopy()
        self.assertEquals(hash(self.dc), hash(b))
        filename = "some_namespace\tuniverse.fo.nodes@0000000001.000000"
        b.add_file(filename, "some_hash")
        present = b.name_is_present(filename)
        self.assertTrue(present)
        self.assertEquals(hash(self.dc), hash(b))
        del b

    def test_reset_instance(self):
        """reset the DataCopy instance

        """
        b = DataCopy()
        filename = "some_namespace\tuniverse.fo.nodes@0000000001.000000"
        b.add_file(filename, "some_hash")
        present = b.name_is_present(filename)
        self.assertTrue(present)
        b._reset()
        present = b.name_is_present(filename)
        self.assertFalse(present)
        del b

    def test_add_file(self):
        """add a file to local copy

        """
        # if nothing was added nothing can be there
        not_present = self.dc.name_is_present("no_such_file")
        self.assertFalse(not_present)

        # adding invalid file
        self.dc.add_file("some_file", "some_hash")  # wrong format
        not_present = self.dc.name_is_present("some_file")
        self.assertFalse(not_present)

        # adding valid file
        filename = "some_namespace\tuniverse.fo.nodes@0000000001.000000"
        self.dc.add_file(filename, "some_hash")
        present = self.dc.name_is_present(filename)
        self.assertTrue(present)

    def test_add_every_typical_filetype(self):
        """add all possible file types

        """
        nodes = "some_namespace\tuniverse.fo.nodes@0000000001.000000"
        elements_c3d6 = "some_namespace\tuniverse.fo.elements.c3d6@0000000001.000000"
        elements_c3d8 = "some_namespace\tuniverse.fo.elements.c3d8@0000000001.000000"
        nodal_field = "some_namespace\tuniverse.fo.nodal.fieldname@0000000001.000000"
        elemental_field_c3d6 = "some_namespace\tuniverse.fo.elemental.c3d6.fieldname@0000000001.000000"
        elemental_field_c3d8 = "some_namespace\tuniverse.fo.elemental.c3d8.fieldname@0000000001.000000"
        surface_skin_c3d6 = "some_namespace\tuniverse.fo.skin.c3d6@0000000001.000000"
        surface_skin_c3d8 = "some_namespace\tuniverse.fo.skin.c3d8@0000000001.000000"
        elset_c3d6 = "some_namespace\tuniverse.fo.elset.c3d6@0000000001.000000"
        elset_c3d8 = "some_namespace\tuniverse.fo.elset.c3d8@0000000001.000000"
        arbitraty_hash = ""
        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            self.dc.add_file(filename, arbitraty_hash)
        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8,
                surface_skin_c3d6, surface_skin_c3d8,
                elset_c3d6, elset_c3d8
        ]:
            present = self.dc.name_is_present(filename)
            self.assertTrue(present)

    def test_get_index(self):
        """get the index of the datacopy

        """
        nodes = "some_namespace\tuniverse.fo.nodes@0000000001.000000"
        elements_c3d6 = "some_namespace\tuniverse.fo.elements.c3d6@0000000001.000000"
        elements_c3d8 = "some_namespace\tuniverse.fo.elements.c3d8@0000000001.000000"
        nodal_field = "some_namespace\tuniverse.fo.nodal.fieldname@0000000001.000000"
        elemental_field_c3d6 = "some_namespace\tuniverse.fo.elemental.c3d6.fieldname@0000000001.000000"
        elemental_field_c3d8 = "some_namespace\tuniverse.fo.elemental.c3d8.fieldname@0000000001.000000"
        arbitraty_hash = ""
        for filename in [
                nodes,
                elements_c3d6, elements_c3d8,
                nodal_field,
                elemental_field_c3d6, elemental_field_c3d8
        ]:
            self.dc.add_file(filename, arbitraty_hash)

        # get whole index
        expected_index = {
            'some_namespace':
            {'0000000001.000000':
             {'nodes':
              {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''},
              'elements':
              {'c3d6':
               {'object_key': 'universe.fo.elements.c3d6@0000000001.000000', 'sha1sum': ''},
               'c3d8': {'object_key': 'universe.fo.elements.c3d8@0000000001.000000', 'sha1sum': ''}
              },
              'nodal':
              {'fieldname': {'object_key': 'universe.fo.nodal.fieldname@0000000001.000000', 'sha1sum': ''}
              },
              'elemental':
              {'fieldname':
               {'c3d6':
                {'object_key': 'universe.fo.elemental.c3d6.fieldname@0000000001.000000', 'sha1sum': ''},
                'c3d8':
                {'object_key': 'universe.fo.elemental.c3d8.fieldname@0000000001.000000', 'sha1sum': ''}
               }
              }
             }
            }
        }
        whole_index = self.dc.get_index()
        self.assertEquals(whole_index, expected_index)

        # get just 'some_namespace'
        expected_index = {
            '0000000001.000000':
            {'nodes':
             {'object_key': 'universe.fo.nodes@0000000001.000000', 'sha1sum': ''},
             'elements':
             {'c3d6':
              {'object_key': 'universe.fo.elements.c3d6@0000000001.000000', 'sha1sum': ''},
              'c3d8': {'object_key': 'universe.fo.elements.c3d8@0000000001.000000', 'sha1sum': ''}
             },
             'nodal':
             {'fieldname': {'object_key': 'universe.fo.nodal.fieldname@0000000001.000000', 'sha1sum': ''}
             },
             'elemental':
             {'fieldname':
              {'c3d6':
               {'object_key': 'universe.fo.elemental.c3d6.fieldname@0000000001.000000', 'sha1sum': ''},
               'c3d8':
               {'object_key': 'universe.fo.elemental.c3d8.fieldname@0000000001.000000', 'sha1sum': ''}
              }
             }
            }
        }
        namespace_index = self.dc.get_index("some_namespace")
        self.assertEquals(namespace_index, expected_index)

        # get non existing namespace
        no_index = self.dc.get_index("xyz")
        self.assertIsNone(no_index)

if __name__ == '__main__':
    unittest.main(verbosity=2)

