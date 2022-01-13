import unittest

import pandas as pd

from etl import transform


class ETLTests(unittest.TestCase):

    # def test_extract(self):

    def test_transform(self):

        # test case covid_df empty
        self.assertEqual(transform(pd.DataFrame(), pd.DataFrame()), RuntimeError)

        # test case with values
        covid_df = pd.DataFrame(
            {'location': ['Germany', 'Egypt', 'Egypt'], 'date': ['2022-01-13', '2022-01-12', '2022-01-12']})
        variant_df = pd.DataFrame(
            {'location': ['Germany', 'Egypt', 'Egypt'], 'date': ['2022-01-13', '2022-01-12', '2022-01-12'],
             'variant': ['corona', 'omicron', 'b2'], 'num_sequences': ['1', '2', '3']}
        )
        expected_result = pd.DataFrame(
            {'location': ['Germany', 'Egypt', 'Egypt'], 'date': ['2022-01-13', '2022-01-12', '2022-01-12'],
             'variant': ['corona', 'b2', 'b2'], 'max_sequences': ['1', '3', '3']}
        )

        self.assertEqual(transform(covid_df, variant_df), expected_result)

        # test case variants df empty
        self.assertEqual(transform(covid_df, pd.DataFrame()), covid_df)