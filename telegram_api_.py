import pandas as pd
df = pd.read_csv('/Users/svetlanamustafina/Downloads/raw_data-8.csv')
print(df.columns)
print(df.geo_object_id.unique().shape)
