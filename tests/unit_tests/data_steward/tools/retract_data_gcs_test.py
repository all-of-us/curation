import unittest


class RetractDataGcsTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        print('**************************************************************')
        print(cls.__name__)
        print('**************************************************************')

    def setUp(self):
        pass

    def test_get_int(self):
        self.assertEqual(int('100'), 100)
        self.assertEqual(int('0'), 0)
        self.assertEqual(int('-1'), -1)
        self.assertRaises(ValueError, int, '1.999')
        self.assertRaises(ValueError, int, 'True')
