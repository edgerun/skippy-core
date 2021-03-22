import unittest

from skippy.core.utils import parse_size_string


class ParseSizeStringTest(unittest.TestCase):
    def test_parse_size_string(self):
        self.assertEqual(1, parse_size_string('1'))
        self.assertEqual(1_000, parse_size_string('1K'))
        self.assertEqual(1_000_000, parse_size_string('1M'))
        self.assertEqual(1_048_576, parse_size_string('1Mi'))

    def test_parse_size_string_error(self):
        self.assertRaises(ValueError, parse_size_string, 'foo')
        self.assertRaises(ValueError, parse_size_string, '')
