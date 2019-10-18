from context import consumer
import unittest
from unittest.mock import patch

from dotted_dict import DottedDict

def _start_dummy_connection(instance):
	pass


class ConsumerTests(unittest.TestCase):

	@patch('consumer.Consumer._start_connection', _start_dummy_connection)
	def test_file_save(self):
		c = consumer.Consumer()

		c._writeToFile(DottedDict({'header': 'fname=test.txt', 'data': 'This is test'.encode()}))
		with open('output/test.txt') as f:
			s = f.readline()
			self.assertEqual(s, 'This is test')


if __name__ == "__main__":
    unittest.main()