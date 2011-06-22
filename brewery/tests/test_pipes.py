#!/usr/bin/env python
# -*- coding: utf-8 -*-

import unittest
import threading
import time
import brewery.streams as streams

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
            if self.pipe.closed():
                break
            self.sent_count += 1
            time.sleep(0.01)
            self.pipe.put(i)

    def source_function(self):
        self.send_sample(1000)
        self.pipe.done_sending()

    def source_limit_function(self):
        self.send_sample_limit_watch(1000)
        self.pipe.done_sending()

    
    def target_function(self):
        self.processed_count = 0
        for value in self.pipe.rows():
            self.processed_count += 1

    def target_limit_function(self):
        self.processed_count = 0
        for value in self.pipe.rows():
            self.processed_count += 1
            if self.processed_count >= 20:
                break
        self.pipe.done_receiving()


    def test_put_get(self):
        self.pipe = streams.Pipe(buffer_size = 10)
        src = threading.Thread(target=self.source_function)
        target = threading.Thread(target=self.target_function)
        src.start()
        target.start()
        target.join()
        src.join()
        self.assertEqual(self.processed_count, 1000)

    def test_early_get_finish(self):
        self.pipe = streams.Pipe(buffer_size = 10)
        src = threading.Thread(target=self.source_function)
        target = threading.Thread(target=self.target_limit_function)
        src.start()
        target.start()
        target.join()
        src.join()
        self.assertEqual(self.processed_count, 20)
        # self.assertEqual(self.sent_count, 1000)

    def test_early_get_finish_watched(self):
        self.pipe = streams.Pipe(buffer_size = 10)
        src = threading.Thread(target=self.source_limit_function)
        target = threading.Thread(target=self.target_limit_function)
        src.start()
        target.start()
        target.join()
        self.assertEqual(self.processed_count, 20)
        self.assertLess(self.sent_count, 1000)

class Pipe2TestCase(unittest.TestCase):

    def setUp(self):
        self.pipe = streams.Pipe(100)

    def stest_put_one(self):
        self.pipe.put(1)
        self.pipe.done_sending()
        for row in self.pipe.rows():
            pass
        self.assertEqual(1, row)

    def test_pget_one(self):
        for i in range(1,100):
            self.pipe.put(i)
        self.pipe.done_sending()

        row = None

        for row in self.pipe.rows():
            break

        self.assertEqual(1, row)

    def producer(self, count = 100, stop = None):
        from random import random as _random
        from time import sleep as _sleep

        counter = 0
        while counter < count:
            self.pipe.put(counter)
            _sleep(_random() * 0.00001)
            counter = counter + 1
        self.pipe.done_sending()

    def consumer(self, count = None):
        self.consumed_count = 0
        for row in self.pipe.rows():
            self.consumed_count += 1
            if count and self.consumed_count >= count:
                break
        self.pipe.done_receiving()

    def test_sending(self):
        producer = threading.Thread(target = self.producer)
        consumer = threading.Thread(target = self.consumer)
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()
        self.assertEqual(100, self.consumed_count)

        self.pipe = streams.Pipe(100)
        producer = threading.Thread(target = self.producer, kwargs = {"count": 200})
        consumer = threading.Thread(target = self.consumer)
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()
        self.assertEqual(200, self.consumed_count)

        self.pipe = streams.Pipe(100)
        producer = threading.Thread(target = self.producer, kwargs = {"count": 150})
        consumer = threading.Thread(target = self.consumer)
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()
        self.assertEqual(150, self.consumed_count)

    def test_receiving(self):
        self.pipe = streams.Pipe(100)
        producer = threading.Thread(target = self.producer, kwargs = {"count": 15})
        consumer = threading.Thread(target = self.consumer, kwargs = {"count": 5})
        producer.start()
        consumer.start()
        producer.join()
        consumer.join()
        self.assertEqual(5, self.consumed_count)
