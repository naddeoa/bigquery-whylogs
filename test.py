import argparse
from functools import reduce
import logging
import re
from typing import Any, Dict, List, Optional, Union
from xmlrpc.client import DateTime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

from apache_beam.transforms.combiners import Sample
from apache_beam.transforms.window import TimestampedValue


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        from whylogs.core import DatasetProfile, DatasetProfileView
        from datetime import datetime

        class WhylogsCombine(beam.CombineFn):
            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile(dataset_timestamp=0).view()

            def add_input(self, accumulator: DatasetProfileView, input: Dict[str, Any]):
                profile = DatasetProfile()
                profile.track(input)
                return accumulator.merge(profile.view())

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                view: DatasetProfileView = DatasetProfile().view()
                for current_view in accumulators:
                    view = view.merge(current_view)
                return view

            def extract_output(self, accumulator: DatasetProfileView) -> DatasetProfileView:
                return accumulator.serialize()

        def to_day_start(milli_time: int) -> datetime:
            date = datetime.fromtimestamp(milli_time/1000.0)
            return date.replace(second=0, microsecond=0, minute=0, hour=0)

        def to_day_start_millis(milli_time: Optional[int]) -> Optional[int]:
            if milli_time is None:
                return 0

            date = to_day_start(milli_time)
            return int(date.timestamp()*1000)

        table_spec = bigquery.TableReference(
            projectId='whylogs-359820',
            datasetId='hacker_news',
            # tableId='short'
            tableId='full_201510'
        )
        query = 'SELECT * FROM `whylogs-359820.hacker_news.full_201510` LIMIT 10'
        hacker_news_data = (
            p
            | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
            # | 'With timestamps' >> beam.Map(lambda row: TimestampedValue(row, row['time']))
            | 'Add keys' >> beam.Map(lambda row: (to_day_start_millis(row['time']), row))
            | 'Profile' >> beam.CombinePerKey(WhylogsCombine())
            # | 'GroupByDay' >> beam.GroupBy(lambda row: to_day_start_millis(row['time']))
            # | 'Profile' >> beam.CombineValues(WhylogsCombine())
        )
        # output = hacker_news_data | 'Format' >> beam.Map(lambda x: f'{x}')
        hacker_news_data | 'Write' >> WriteToText(known_args.output)


def stuff():
    schema = DatasetSchema(types={'by': str})
    profile = DatasetProfile(schema=schema)
    data = {'by': 'HSO'}
    profile.track(row=data)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # stuff()
    run()
