from typing import Dict, List, Optional
import pickle
import pandas as pd
from whylogs.core import DatasetProfile, DatasetProfileView
import whylogs as why


class ProfileIndex():

    def __init__(self, index: Dict[str, DatasetProfileView] = {}) -> None:
        self.index: Dict[str, DatasetProfileView] = index

    def get(self, date_str: str) -> Optional[DatasetProfileView]:
        return self.index[date_str]

    def set(self, date_str: str, view: DatasetProfileView):
        self.index[date_str] = view

    # Mutates
    def merge_index(self, other: 'ProfileIndex') -> 'ProfileIndex':
        for date_str, view in other.index.items():
            self.merge(date_str, view)

        return self

    def merge(self, date_str: str, view: DatasetProfileView):
        if date_str in self.index:
            self.index[date_str] = self.index[date_str].merge(view)
        else:
            self.index[date_str] = view

    def estimate_size(self) -> int:
        return sum(map(len, self.extract().values()))

    def __len__(self) -> int:
        return len(self.index)

    def __iter__(self):
        # The runtime wants to use this to estimate the size of the object,
        # I suppose to load balance across workers.
        return self.extract().values().__iter__()

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, d):
        self.__dict__ = d

    def extract(self) -> Dict[str, bytes]:
        out: Dict[str, bytes] = {}
        for date_str, view in self.index.items():
            out[date_str] = view.serialize()
        return out


def profile() -> ProfileIndex:
    # df = pd.read_csv("data.csv")
    df = pd.read_csv("data_short.csv")

    df['time'] = pd.to_datetime(df["time"], unit='s')

    grouped = df.set_index('time').groupby(pd.Grouper(freq='D'))

    # TODO going to have to update the pipeline to do this in the batch processing call,
    # and I'll have to return a dict to keep track of which profiles to merge with which profiles,
    # and update the CombineFn to work for that Dict

    profiles = ProfileIndex()
    for date_group, dataframe in grouped:
        # pandas includes every date in the range, not just the ones that had rows...
        if len(dataframe) == 0:
            continue

        ts = date_group.to_pydatetime()
        profile = DatasetProfile(dataset_timestamp=ts)
        profile.track(dataframe)
        profiles.set(date_group, profile.view())

    return profiles


def merge_profiles(profiles: List[ProfileIndex]) -> ProfileIndex:
    merged = ProfileIndex()

    for index in profiles:
        merged.merge_index(index)

    return merged


if __name__ == "__main__":
    index = profile()
    merged = merge_profiles([index, index])

    merged2 = pickle.loads(pickle.dumps(merged))
    print(merged2.__dict__)
