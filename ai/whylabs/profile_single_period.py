import logging
import re
import random
from math import ceil
from typing import Any, Dict, Iterator, List, Optional, Tuple
from whylogs.core import DatasetProfile, DatasetProfileView
import pandas as pd

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.typehints.batch import BatchConverter, ListBatchConverter

from apache_beam.options.value_provider import RuntimeValueProvider, NestedValueProvider, StaticValueProvider

# matches PROJECT:DATASET.TABLE.
table_ref_regex = re.compile(r'[^:\.]+:[^:\.]+\.[^:\.]+')


class ProfileIndex():
    """
    Abstraction around the type of thing that we return from profiling rows. It represents
    a dictionary of dataset timestamp to whylogs ResultSet.
    """

    def __init__(self, index: Dict[str, DatasetProfileView] = {}) -> None:
        self.index: Dict[str, DatasetProfileView] = index

    def get(self, date_str: str) -> Optional[DatasetProfileView]:
        return self.index[date_str]

    def set(self, date_str: str, view: DatasetProfileView):
        self.index[date_str] = view

    def tuples(self) -> List[Tuple[str, DatasetProfileView]]:
        return list(self.index.items())

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


class WhylogsProfileIndexMerger(beam.CombineFn):
    def __init__(self, logging_level: RuntimeValueProvider):
        self.logger = logging.getLogger("WhylogsProfileIndexMerger")
        self.logging_level = logging_level

    def setup(self):
        self.logger.setLevel(logging.getLevelName(self.logging_level.get()))

    def create_accumulator(self) -> ProfileIndex:
        return ProfileIndex()

    def add_input(self, accumulator: ProfileIndex, input: ProfileIndex) -> ProfileIndex:
        return accumulator.merge_index(input)

    def add_inputs(self, mutable_accumulator: ProfileIndex, elements: List[ProfileIndex]) -> ProfileIndex:
        count = 0
        for current_index in elements:
            mutable_accumulator.merge_index(current_index)
            count = count + 1
        self.logger.debug("adding %s inputs", count)
        return mutable_accumulator

    def merge_accumulators(self, accumulators: List[ProfileIndex]) -> ProfileIndex:
        acc = ProfileIndex()
        count = 0
        for current_index in accumulators:
            acc.merge_index(current_index)
            count = count + 1
        self.logger.debug("merging %s views", count)
        return acc

    def extract_output(self, accumulator: ProfileIndex) -> Dict[str, bytes]:
        return accumulator.extract()


class ProfileDoFn(beam.DoFn):
    def __init__(self, date_column: RuntimeValueProvider, date_grouping_frequency: RuntimeValueProvider, logging_level: RuntimeValueProvider):
        self.date_column: RuntimeValueProvider = date_column
        self.freq: RuntimeValueProvider = date_grouping_frequency
        self.logger = logging.getLogger("ProfileDoFn")
        self.logging_level = logging_level

    def setup(self):
        self.logger.setLevel(logging.getLevelName(self.logging_level.get()))

    # TODO convert this so that people don't need to have a datetime explicitly listed
    def _process_batch_without_date(self, batch: List[Dict[str, Any]]) -> Iterator[List[DatasetProfileView]]:
        self.logger.debug("Processing batch of size %s", len(batch))
        profile = DatasetProfile()
        profile.track(pd.DataFrame.from_dict(batch))
        yield [profile.view()]

    def _process_batch_with_date(self, batch: List[Dict[str, Any]]) -> Iterator[List[ProfileIndex]]:
        tmp_date_col = '_whylogs_datetime'
        df = pd.DataFrame(batch)
        df[tmp_date_col] = pd.to_datetime(df[self.date_column.get()])
        grouped = df.set_index(tmp_date_col).groupby(
            pd.Grouper(freq=self.freq.get()))

        profiles = ProfileIndex()
        for date_group, dataframe in grouped:
            # pandas includes every date in the range, not just the ones that had rows...
            # TODO find out how to make the dataframe exclude empty entries instead
            if len(dataframe) == 0:
                continue

            ts = date_group.to_pydatetime()
            profile = DatasetProfile(dataset_timestamp=ts)
            profile.track(dataframe)
            profiles.set(str(date_group), profile.view())

        self.logger.debug("Processing batch of size %s into %s profiles", len(
            batch), len(profiles))

        # TODO best way of returning this thing is pickle?
        yield [profiles]

    def process_batch(self, batch: List[Dict[str, Any]]) -> Iterator[List[ProfileIndex]]:
        return self._process_batch_with_date(batch)


class TemplateArguments(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--output',
            dest='output',
            help='Output file or gs:// path to write results to.')
        parser.add_value_provider_argument(
            '--input',
            dest='input',
            help='This can be a SQL query that includes a table name or a fully qualified reference to a table with the form PROJECT:DATASET.TABLE')
        parser.add_value_provider_argument(
            '--date_column',
            dest='date_column',
            help='The string name of the column that contains a datetime. The column should be of type TIMESTAMP in the SQL schema.')
        parser.add_value_provider_argument(
            '--date_grouping_frequency',
            dest='date_grouping_frequency',
            default='D',
            help='One of the freq options in the pandas Grouper(freq=) API. D for daily, W for weekly, etc.')
        parser.add_value_provider_argument(
            '--logging_level',
            dest='logging_level',
            default='INFO',
            help='One of the logging levels from the logging module.')


def is_table_input(table_string: str) -> bool:
    return table_ref_regex.match(table_string) is not None


def resolve_table_input(input: str):
    return input if is_table_input(input) else None


def resolve_query_input(input: str):
    return None if is_table_input(input) else input


def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions()
    template_arguments = pipeline_options.view_as(TemplateArguments)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    table_input = NestedValueProvider(
        template_arguments.input, resolve_table_input)
    query_input = NestedValueProvider(
        template_arguments.input, resolve_query_input)

    with beam.Pipeline(options=pipeline_options) as p:
        # import pandas as pd
        # from whylogs.core import DatasetProfile, DatasetProfileView

        class ProfileIndexBatchConverter(ListBatchConverter):
            # TODO why do I get an error around this stuff while uploading the template now?
            def estimate_byte_size(self, batch: List[ProfileIndex]):
                if len(batch) == 0:
                    return 0

                return batch[0].estimate_size()

        BatchConverter.register(ProfileIndexBatchConverter)

        logging_level = template_arguments.logging_level
        date_column = template_arguments.date_column
        date_grouping_frequency = template_arguments.date_grouping_frequency
        result = (
            p
            | 'ReadTable' >> beam.io.ReadFromBigQuery(query=query_input, use_standard_sql=True)
            .with_output_types(Dict[str, Any])
            | 'Profile' >> beam.ParDo(ProfileDoFn(date_column, date_grouping_frequency, logging_level))
            | 'Merge profiles' >> beam.CombineGlobally(WhylogsProfileIndexMerger(logging_level))
        )

        result | 'Upload to WhyLabs' >> beam.ParDo(lambda x: x)

        result | 'Write' >> WriteToText(template_arguments.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
