import unittest
import zipfile

from scripts.build_package import build


class PackageTests(unittest.TestCase):
    def test_archive_contains_package(self):
        names = zipfile.ZipFile(build()).namelist()
        for expected in ("catalog_etl/__init__.py", "catalog_etl/config.py", "catalog_etl/transform.py"):
            self.assertIn(expected, names)


if __name__ == "__main__":
    unittest.main()
