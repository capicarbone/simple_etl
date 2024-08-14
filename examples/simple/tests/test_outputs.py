import unittest
from main import avg_speed_mph, kmp_to_mph, lap_time, max_speed_mph, min_speed_mph


class OutputTestCase(unittest.TestCase):

    def test_min_speed_mph(self):

        self.assertEqual("200.1", min_speed_mph("322"))
        self.assertEqual("49.8", min_speed_mph("80.2"))

    def test_max_speed_mph(self):

        self.assertEqual("200.1", max_speed_mph("322"))
        self.assertEqual("49.8", max_speed_mph("80.2"))

    def test_avg_speed_mph(self):

        self.assertEqual("200.1", avg_speed_mph("322"))
        self.assertEqual("49.8", avg_speed_mph("80.2"))

    def test_lap_time(self):

        times = ["00:00:30.100", "00:00:30.100", "00:00:30.100"]

        self.assertEqual("00:01:30.300", lap_time(*times))


class UtilsTestCase(unittest.TestCase):

    def test_kmp_to_mph(self):
        self.assertEqual(200.1, kmp_to_mph(322))
        self.assertEqual(49.8, kmp_to_mph(80.2))
        self.assertEqual(62.1, kmp_to_mph(100))
