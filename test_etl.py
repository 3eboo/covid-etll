import pandas as pd
import pytest

from etl import transform


def test_transform():
    # test case covid_df empty
    with pytest.raises(RuntimeError):
        transform.run(pd.DataFrame(), pd.DataFrame())

    # test case with values
    covid_df = pd.DataFrame(
        {'location': ['Egypt', 'Egypt', 'Egypt', 'Germany', 'Germany', 'Germany'],
         'date': ['2022-01-12', '2022-01-13', '2022-01-14', '2022-01-12', '2022-01-13', '2022-01-14']})
    variant_df = pd.DataFrame(
        {'location': ['Germany', 'Germany', 'Germany', 'Germany', 'Germany', 'Germany', 'Germany', 'Germany',
                      'Germany', 'Egypt', 'Egypt', 'Egypt', 'Egypt', 'Egypt', 'Egypt', 'Egypt', 'Egypt', 'Egypt'],
         'date': ['2022-01-12', '2022-01-13', '2022-01-14', '2022-01-12', '2022-01-13', '2022-01-14', '2022-01-12',
                  '2022-01-13', '2022-01-14', '2022-01-12', '2022-01-13', '2022-01-14', '2022-01-12', '2022-01-13',
                  '2022-01-14', '2022-01-12', '2022-01-13', '2022-01-14'],
         'variant': ['corona', 'omicron', 'b2', 'omicron', 'b2', 'corona', 'b2', 'corona', 'omicron',
                     'corona', 'omicron', 'b2', 'omicron', 'b2', 'corona', 'b2', 'corona', 'omicron'],
         'num_sequences': [8, 3, 9, 15, 0, 6, 1, 5, 10, 8, 7, 12, 4, 0, 0, 8, 2, 2]}
    )

    expected_result = pd.DataFrame(
        {'location': ['Egypt', 'Egypt', 'Egypt', 'Germany', 'Germany', 'Germany'],
         'date': ['2022-01-12', '2022-01-13', '2022-01-14', '2022-01-12', '2022-01-13', '2022-01-14'],
         'variant': ['corona', 'omicron', 'b2', 'omicron', 'corona', 'omicron'],
         'max_sequences': [8, 7, 12, 15, 5, 10]}
    )
    assert transform.run(covid_df, variant_df).equals(expected_result)

    # test case empty variants df
    assert transform.run(covid_df, pd.DataFrame()).equals(covid_df)
