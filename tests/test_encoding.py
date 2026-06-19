

import unittest
from twainmq.encoding import base85_to_key, key_to_base85


class TestBase85Encoding(unittest.TestCase):
    def test_round_trip_small_numbers(self):
        for n in [0, 1, 42, 255, 256, 12345]:
            enc = key_to_base85(n, width=2)
            dec = base85_to_key(enc, width=2)
            self.assertEqual(dec, n)

    def test_round_trip_large_numbers(self):
        # Max 64-bit unsigned integer
        n = 2**64 - 1
        enc = key_to_base85(n, width=8)
        dec = base85_to_key(enc, width=8)
        self.assertEqual(dec, n)

    def test_fixed_length_output(self):
        n = 123
        enc = key_to_base85(n, width=1)
        self.assertEqual(len(enc), 2)
        n = 123456789
        enc = key_to_base85(n, width=4)
        self.assertEqual(len(enc), 5)
        enc = key_to_base85(n, width=8)
        self.assertEqual(len(enc), 10)

    def test_different_numbers_produce_different_encodings(self):
        enc1 = key_to_base85(123, width=1)
        enc2 = key_to_base85(124, width=1)
        self.assertNotEqual(enc1, enc2)
