import unittest

from price_load.config import jdbc_options_from_env, validate_date

ENV = {"PG_HOST": "h", "PG_PORT": "5432", "PG_DB": "d", "PG_USER": "u", "PG_PASSWORD": "p"}


class ConfigTests(unittest.TestCase):
    def test_missing_vars(self):
        with self.assertRaises(RuntimeError):
            jdbc_options_from_env({}, "ref.prices")

    def test_url(self):
        self.assertEqual(jdbc_options_from_env(ENV, "t")["url"], "jdbc:postgresql://h:5432/d")

    def test_date(self):
        self.assertEqual(validate_date("2026-01-15"), "2026-01-15")
        with self.assertRaises(ValueError):
            validate_date("15.01.2026")


if __name__ == "__main__":
    unittest.main()
