"""
Test for :py:mod:`slipoframes.context` module
"""
import unittest

from config import SlipoContext, SlipoException  # pylint: disable=import-error

from secret import BASE_URL, API_KEY


class TestContext(unittest.TestCase):

    def test_client_requires_ssl_true_exception(self):
        def create(): return SlipoContext(
            api_key=API_KEY,
            base_url='http://127.0.0.1',
        )

        self.assertRaises(SlipoException, create)

    def test_client_validation_error_exception(self):
        def create(): return SlipoContext(
            api_key=API_KEY,
            base_url='http://127.0.0.1',
            requires_ssl=False,
        )

        self.assertRaises(SlipoException, create)


if __name__ == '__main__':
    unittest.main()
