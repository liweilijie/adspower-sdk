# tests/test_adspower.py
import unittest
from adspower.adspowerapi import AdspowerAPI

class TestAdspowerAPI(unittest.TestCase):
    def test_example(self):
        api = AdspowerAPI()
        self.assertIsNotNone(api)
