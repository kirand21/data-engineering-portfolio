import unittest
import zipfile

from scripts.build_package import build


class PackageTests(unittest.TestCase):
    def test_archive_contains_package(self):
        names = zipfile.ZipFile(build()).namelist()
        for expected in ("price_load/__init__.py", "price_load/config.py", "price_load/transform.py"):
            self.assertIn(expected, names)


if __name__ == "__main__":
    unittest.main()
