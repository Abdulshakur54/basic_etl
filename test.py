import pandas as pd
from index import clean_data
bad_data = {
        'carat': [0.23,0.21,0.24],
        'cut':  [None, 'Premium', 'Good'],
        'color': ['E', 'E', 'I']
    }
bad_data_df = pd.DataFrame(bad_data) 
manual = bad_data_df.iloc[1:]
auto = clean_data(bad_data_df)
print(auto.equals(manual))


