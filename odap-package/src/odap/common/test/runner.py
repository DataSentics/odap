import os
import unittest

from pathlib import Path


def run_test(test_case):
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(test_case)
    return unittest.TextTestRunner().run(suite)


def import_all_test_modules():
    for module in os.listdir(f"{Path.cwd()}/tests"):
        if module[-3:] == ".py":
            __import__(f"tests.{module[:-3]}", locals(), globals())


def run_all_tests():
    import_all_test_modules()
