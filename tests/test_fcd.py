
import unittest

from wri_list_tools import ForestChangeDiagnostic

input_tsv = "tests/palm-generic-32.tsv"
fcd_output = "tests/fcd_output"


class TestListUtils(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fcd = ForestChangeDiagnostic(fcd_output)

    def test_read_list_tsv(self):
        self.assertEqual(len(self.fcd.df), 33)

    def test_idn_landcover_max_year(self):
        df = self.fcd['tree_cover_loss_idn_landcover_yearly']
        max_year = df.reset_index()['year'].max()
        self.assertEqual(int(max_year), 2020)

    def test_soy_tree_loss_max_year(self):
        df = self.fcd['commodity_threat_deforestation']
        max_year = df.reset_index()['year'].max()
        # commodity threat is locked to 2019, cool!
        self.assertEqual(int(max_year), 2019)
