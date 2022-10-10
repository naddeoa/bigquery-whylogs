from sqlite3 import TimeFromTicks
from whylogs.core import DatasetProfile, DatasetProfileView
import pandas as pd
from timeit import default_timer as timer


df = pd.read_csv('data.csv')
rows = df.to_dict('records')


start = timer()
profile = DatasetProfile()
profile.track(df)
view = profile.view()
end = timer()
print(view.to_pandas())
print(f'Took {end - start} seconds for df')


new_df = pd.DataFrame.from_dict(rows)
start = timer()
profile = DatasetProfile()
profile.track(new_df)
view = profile.view()
end = timer()
print(view.to_pandas())
print(f'Took {end - start} seconds for from_rows df')


start = timer()
profile = DatasetProfile()
for row in rows:
    profile.track(row)

view = profile.view()
end = timer()
print(f'Took {end - start} seconds for rows')
