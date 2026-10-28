import io
import unittest

from sim.progress import Progress


class ProgressTests(unittest.TestCase):
    def test_compact_progress_and_completion(self):
        stream = io.StringIO()
        progress = Progress(3600, stream)
        progress.update(0)
        progress.update(300.9)
        progress.update(300.99)
        progress.update(4000)
        progress.finish()
        progress.close()
        self.assertEqual(stream.getvalue(),
                         "\r0s/3600s\r300s/3600s\r3599s/3600s\r3600s/3600s\n")

    def test_failed_run_does_not_show_complete(self):
        stream = io.StringIO()
        progress = Progress(100, stream)
        progress.update(20)
        progress.close()
        self.assertEqual(stream.getvalue(), "\r20s/100s\n")


if __name__ == "__main__":
    unittest.main()
