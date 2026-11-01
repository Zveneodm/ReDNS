import unittest

from sim.clock import ManualClock
from src.cooperation import CopyDirectory
from src.models import ReDNSForwarder
from src.records import Record


class DirectoryTests(unittest.TestCase):
    def test_actual_store_update_and_remove_counts(self):
        directory = CopyDirectory()
        directory.store(0, 1, 10)
        directory.store(0, 1, 20)
        directory.store(1, 1, 30)
        self.assertEqual(directory.entries, 2)
        self.assertEqual(dict(directory.child_counts), {0: 1, 1: 1})
        directory.remove(0, 1, 20)
        self.assertEqual(directory.choose(1, 0), 1)
        directory.remove(1, 1, 30)
        self.assertEqual(directory.entries, 0)
        self.assertFalse(directory.by_name)
        self.assertFalse(directory.child_counts)

    def test_lookup_prunes_expired_hints_and_selects_smallest_id(self):
        directory = CopyDirectory()
        for child, expiry in ((0, 1), (5, 100), (3, 20)):
            directory.store(child, 10, expiry)
        self.assertEqual(directory.choose(10, 1), 3)
        self.assertEqual(directory.entries, 2)
        self.assertIsNone(directory.choose(10, 100))
        self.assertEqual(directory.entries, 0)

    def test_old_delete_does_not_remove_later_deadline(self):
        directory = CopyDirectory()
        directory.store(0, 1, 100)
        directory.remove(0, 1, 10)
        self.assertEqual(directory.deadline(0, 1), 100)

    def test_transfer_skips_existing_copy(self):
        node = ReDNSForwarder(1, ManualClock())
        node.copy_directory.store(2, 7, 100)
        self.assertIsNone(node.transfer_target(Record(7, 100, 100), range(4), 2))
        self.assertEqual(node.stats["transfer_skipped_known_copies"], 1)

    def test_transfer_uses_only_connected_clients_with_spare_hints(self):
        node = ReDNSForwarder(1, ManualClock())
        for child in (0, 2, 4):
            node.copy_directory.store(child, child, 100)
        self.assertIsNone(node.transfer_target(Record(7, 100, 100), range(0, 6, 2), 1))
        node.copy_directory.remove(4, 4)
        self.assertEqual(node.transfer_target(Record(7, 100, 100), range(0, 6, 2), 1), 4)

    def test_transfer_with_no_children_or_capacity_is_skipped(self):
        node = ReDNSForwarder(1, ManualClock())
        self.assertIsNone(node.transfer_target(Record(7, 100, 100), range(0), 1))
        self.assertIsNone(node.transfer_target(Record(7, 100, 100), range(3), 0))
