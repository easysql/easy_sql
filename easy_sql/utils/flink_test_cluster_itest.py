import unittest

from .flink_test_cluster import FlinkTestClusterManager


class FlinkTestClusterManagerTest(unittest.TestCase):
    def test_cluster_manager(self):
        fm = FlinkTestClusterManager(10)
        if fm.is_started():
            fm.stop_cluster()
        fm.start_cluster()
        self.assertTrue(fm.is_started())
        self.assertFalse(fm.is_not_started())
        fm.stop_cluster()
        self.assertFalse(fm.is_started())
        self.assertTrue(fm.is_not_started())
