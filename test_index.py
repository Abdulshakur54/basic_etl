import pytest
import pandas as pd
import unittest.mock as mock
from index import clean_data, append_processing_time, etl_pipeline


@pytest.fixture
def good_data_df():
    return pd.DataFrame({
        'carat': [0.23,0.21,0.24],
        'cut':  ['Ideal', 'Premium', 'Good'],
        'color': ['E', 'E', 'I']
    })
    
    
    
@pytest.fixture
def bad_data_df():
    return pd.DataFrame({
        'carat': [0.23,0.21,0.24],
        'cut':  [None, 'Premium', 'Good'],
        'color': ['E', 'E', 'I']
    })

def test_clean_data(good_data_df,bad_data_df):
    assert clean_data(good_data_df).equals(good_data_df)
    bad_data_expected = bad_data_df.iloc[1:]
    assert clean_data(bad_data_df).equals(bad_data_expected)


def test_append_processing_time(good_data_df):
    response = append_processing_time(good_data_df)
    assert 'INSERTED_AT' in response

@mock.patch('index.mongodb_connection')
def test_etl_pipeline(insert_many_mock, good_data_df):
    assert etl_pipeline(good_data_df) == None
    
    #assert that insert_many method is called and with the right argument
    insert_many_mock.basic_etl.diamonds.insert_many.assert_called_with(good_data_df.to_dict('records'))
 
 
    