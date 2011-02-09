import unittest
import brewery
import brewery.ds as ds
import brewery.pipes as pipes
import csv
import os
import threading
import time

class PipeTestCase(unittest.TestCase):
    def setUp(self):
        self.processed_count = 0
        self.pipe = None
        self.sent_count = 0

    def send_sample(self, sample_size = 10):
        for i in range(0, sample_size):
            self.sent_count += 1
            self.pipe.put(i)

    def send_sample_limit_watch(self, sample_size = 10):
        for i in range(0, sample_size):
            if self.pipe.finished:
                break
            self.sent_count += 1
            time.sleep(0.01)
            self.pipe.put(i)

    def source_function(self):
        self.send_sample(1000)
        self.pipe.flush()

    def source_limit_function(self):
        self.send_sample_limit_watch(1000)
        self.pipe.flush()

    
    def target_function(self):
        self.processed_count = 0
        for value in self.pipe.rows():
            self.processed_count += 1

    def target_limit_function(self):
        self.processed_count = 0
        for value in self.pipe.rows():
            self.processed_count += 1
            if self.processed_count >= 20:
                self.pipe.stop()
                break


    def test_put_get(self):
        self.pipe = pipes.Pipe(buffer_size = 10)
        src = threading.Thread(target=self.source_function)
        target = threading.Thread(target=self.target_function)
        src.start()
        target.start()
        target.join()
        src.join()
        self.assertEqual(self.processed_count, 1000)

    def test_early_get_finish(self):
        self.pipe = pipes.Pipe(buffer_size = 10)
        src = threading.Thread(target=self.source_function)
        target = threading.Thread(target=self.target_limit_function)
        src.start()
        target.start()
        target.join()
        src.join()
        self.assertEqual(self.processed_count, 20)
        # self.assertEqual(self.sent_count, 1000)

    def test_early_get_finish_watched(self):
        self.pipe = pipes.Pipe(buffer_size = 10)
        src = threading.Thread(target=self.source_limit_function)
        target = threading.Thread(target=self.target_limit_function)
        src.start()
        target.start()
        target.join()
        self.assertEqual(self.processed_count, 20)
        self.assertLess(self.sent_count, 1000)
