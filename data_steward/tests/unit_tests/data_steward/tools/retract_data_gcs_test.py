import unittest

from tools import retract_data_gcs as rd


class RetractDataGcsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def test_get_int(self):
        self.assertEqual(rd.get_integer('100'), 100)
        self.assertEqual(rd.get_integer('0'), 0)
        self.assertEqual(rd.get_integer('-1'), -1)
        self.assertRaises(ValueError, rd.get_integer, '1.999')
        self.assertRaises(ValueError, rd.get_integer, 'True')
