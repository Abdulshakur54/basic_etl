from index import etl_csv, csv_file

def test_etl_csv():
    assert etl_csv(csv_file) == True

