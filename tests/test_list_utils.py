import unittest

from wri_list_tools import read_list_tsv, read_job_output

input_tsv = "tests/palm-generic-32.tsv"
fcd_output = "tests/fcd_output"


class TestListUtils(unittest.TestCase):
    def test_read_list_tsv(self):
        df = read_list_tsv(input_tsv).reset_index()
        self.assertEqual(len(df), 33)

    def test_read_job_output(self):
        df = read_job_output(fcd_output).reset_index()
        self.assertEqual(len(df), 33)
