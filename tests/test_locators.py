import unittest

from simple_etl.locators import Column


class ColumnLocatorTest(unittest.TestCase):

    def setUp(self) -> None:
        self.test_record = (("c1", "c2", "c5"), (10, 22, 55))

    def test_valid_location(self):

        locator = Column("c5")
        self.assertEqual(locator.locate(self.test_record), 55)

        locator = Column("c1")
        self.assertEqual(locator.locate(self.test_record), 10)
