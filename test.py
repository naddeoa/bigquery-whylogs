import argparse
from datetime import timedelta
from functools import reduce
import logging
import profile
import re
from typing import Any, Dict, List, Optional, Tuple, Union
from xmlrpc.client import DateTime

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from apache_beam.io.gcp.internal.clients import bigquery

from apache_beam.typehints import WindowedValue
from apache_beam.transforms.combiners import Sample
from apache_beam.transforms.trigger import AfterCount, DefaultTrigger, AfterAny, Repeatedly, AfterProcessingTime, AccumulationMode
from apache_beam.transforms.window import TimestampedValue, WindowFn, FixedWindows, IntervalWindow


def run(argv=None, save_main_session=True):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    parser.add_argument(
        '--method',
        dest='method',
        required=True,
        help='Which pipeline to execute')
    parser.add_argument(
        '--runtime-type-check',
        dest='method',
        default=True,
        required=False)
    known_args, pipeline_args = parser.parse_known_args(argv)

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    with beam.Pipeline(options=pipeline_options) as p:
        from timeit import default_timer as timer
        import pandas as pd
        import cProfile
        import pstats
        import os
        from io import StringIO
        from whylogs.core import DatasetProfile, DatasetProfileView
        from datetime import datetime

        # Apparently adds considerable overhead
        logger = logging.getLogger()

        class WhylogsCombineBulk(beam.CombineFn):
            """
            This combiner depends on GroupIntoBatches running and grouping all of the
            rows into batch sizes so that we end up getting passed lists here.
            """

            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile().view()

            def add_input(self, accumulator: DatasetProfileView, input: List[Dict[str, Any]]) -> DatasetProfileView:
                start = timer()
                logger.info(f'Tracking {len(input)} of type {type(input)}.')
                if len(input) == 0:
                    logger.warn('Got empty add_input')
                    return accumulator

                profile = DatasetProfile()
                profile.track(pd.DataFrame.from_dict(input))
                ret = accumulator.merge(profile.view())
                end = timer()
                total = end - start
                logger.info(
                    f'add_input took {total} seconds with WHYLOGS_NO_ANALYTICS {os.environ["WHYLOGS_NO_ANALYTICS"]}')
                return ret

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                start = timer()
                if len(accumulators) == 1:
                    logger.info('Returning accumulator, only one to merge')
                    return accumulators[0]

                logger.info(f'Merging {len(accumulators)} views together')
                view: DatasetProfileView = DatasetProfile().view()
                for current_view in accumulators:
                    view = view.merge(current_view)
                end = timer()
                total = end - start
                logger.info(f'merge_accumulators took {total} seconds')
                return view

            def extract_output(self, accumulator: DatasetProfileView) -> bytes:
                start = timer()
                ser = accumulator.serialize()
                end = timer()
                total = end - start
                logger.info(f'extract_output took {total} seconds')
                return ser

        class CombineBulkControl(beam.CombineFn):
            """
            This mimics the WhylogsCombineBulk combiner but doesn't use whylogs.
            """

            def create_accumulator(self) -> int:
                return 0

            def add_input(self, accumulator: int, input: List[Dict[str, Any]]) -> int:
                return accumulator + len(input)

            def merge_accumulators(self, accumulators: List[int]) -> int:
                return sum(accumulators)

            def extract_output(self, accumulator: int) -> int:
                return accumulator

        class WhylogsCombineSingle(beam.CombineFn):
            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile(dataset_timestamp=0).view()

            def add_input(self, accumulator: DatasetProfileView, input: Dict[str, Any]) -> DatasetProfileView:
                profile = DatasetProfile()
                profile.track(input)
                return accumulator.merge(profile.view())
                # return 0

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                view: DatasetProfileView = DatasetProfile().view()
                for current_view in accumulators:
                    view = view.merge(current_view)
                return view
                # return 1

            def extract_output(self, accumulator: DatasetProfileView) -> bytes:
                return accumulator.serialize()
                # return 1

        class NoOpCombiner(beam.CombineFn):
            """
            Not getting faster than this
            """

            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile(dataset_timestamp=0).view()

            def add_input(self, accumulator: DatasetProfileView, input: Dict[str, Any]) -> DatasetProfileView:
                return 0

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                return 1

            def extract_output(self, accumulator: DatasetProfileView) -> DatasetProfileView:
                return 1

        class Foo(WindowFn):
            def assign(self, assign_context):
                pass

            def merge(self, merge_context):
                pass

            def get_window_coder(self):
                pass

        class CombineProfiledRows(beam.CombineFn):
            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile(dataset_timestamp=0).view()

            def add_input(self, accumulator: DatasetProfileView, input: DatasetProfileView) -> DatasetProfileView:
                return accumulator.merge(input)

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                # logger.info(f'Merging {len(accumulators)} views together')
                view: DatasetProfileView = DatasetProfile().view()
                for current_view in accumulators:
                    view = view.merge(current_view)
                return view

            def extract_output(self, accumulator: DatasetProfileView) -> DatasetProfileView:
                return accumulator.serialize()

        def to_day_start(milli_time: int) -> datetime:
            date = datetime.fromtimestamp(milli_time)
            return date.replace(second=0, microsecond=0, minute=0, hour=0)

        def to_day_start_millis(milli_time: Optional[int]) -> Optional[int]:
            if milli_time is None:
                return 0
            date = to_day_start(milli_time)
            return int(date.timestamp())

        table_spec = bigquery.TableReference(
            projectId='whylogs-359820',
            datasetId='hacker_news',
            # tableId='short'
            # tableId='full_201510'
            # tableId='comments'
            tableId='comments_half'
        )
        # query = "SELECT * FROM `whylogs-359820.hacker_news.comments_half` order by time"
        # query = "SELECT * FROM `whylogs-359820.hacker_news.comments` order by time"
        # query = "SELECT * FROM `whylogs-359820.hacker_news.short` order by time"

        if known_args.method == '1':
            # Method 1
            # Using group into batches to make each value that we reduce a List[List[Row]],
            # which means our CombineFn ends up getting a List[Row] which lets us track multiple
            # things at once, instead of having to create a single row DatasetProfileView.
            # Takes around 3 hours for the full comments data set (18mil rows, ~4gb)
            # Very very slow: https://console.cloud.google.com/dataflow/jobs/us-central1/2022-10-06_20_56_11-15068493612663192545;step=;mainTab=JOB_GRAPH;bottomTab=WORKER_LOGS;logsSeverity=INFO;graphView=0?project=whylogs-359820&pageState=(%22dfTime%22:(%22d%22:%22PT1H%22))
            # Or 2 hours for half the amount: https://console.cloud.google.com/dataflow/jobs/us-west1/2022-10-08_13_47_12-8948448933760541150;graphView=0?project=whylogs-359820
            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                .with_output_types(Dict[str, Any])
                | 'Add keys' >> beam.Map(lambda row: (to_day_start_millis(row['time']), row))
                .with_output_types(Tuple[int,  Dict[str, Any]])
                | 'Group into batches' >> beam.GroupIntoBatches(10_000, max_buffering_duration_secs=120)
                .with_output_types(Tuple[int,  List[Dict[str, Any]]])
                | 'Profile' >> beam.CombinePerKey(WhylogsCombineBulk())
                .with_output_types(Tuple[int, bytes])
            )

        elif known_args.method == '1-control':
            # Method 1-control
            # Same as 1 but it doesn't use whylogs. Takes about 20 minutes. We should be close to this.
            # https://console.cloud.google.com/dataflow/jobs/us-west1/2022-10-08_10_31_21-14427245417999075919;graphView=0?project=whylogs-359820
            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                .with_output_types(Dict[str, Any])
                | 'Add keys' >> beam.Map(lambda row: (to_day_start_millis(row['time']), row))
                .with_output_types(Tuple[int,  Dict[str, Any]])
                | 'Group into batches' >> beam.GroupIntoBatches(10_000, max_buffering_duration_secs=30)
                .with_output_types(Tuple[int,  List[Dict[str, Any]]])
                | 'Profile' >> beam.CombinePerKey(CombineBulkControl())
                .with_output_types(Tuple[int, int])
            )

        elif known_args.method == '2':
            # Method 2
            # Purposefully profile every row one at a time.
            # Works the best so far. Took a little over an hour.
            # Works a little better than doing method 3 since we're just tracking single rows anyway, but neither of these are great: https://console.cloud.google.com/dataflow/jobs/us-west1/2022-10-07_12_06_50-6303582136449105573;graphView=0?project=whylogs-359820
            # 45 minutes for 4gigs: https://console.cloud.google.com/dataflow/jobs/us-west2/2022-10-08_18_01_52-3803300391562088720;step=;graphView=0?project=whylogs-359820
            def add_keys_and_profile(row: Dict[str, Any]) -> Tuple[int, DatasetProfileView]:
                profile = DatasetProfile()
                profile.track(row)
                return (to_day_start_millis(row['time']), profile.view())

            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                .with_output_types(Dict[str, Any])
                | 'Add keys and profile' >> beam.Map(add_keys_and_profile)
                .with_output_types(Tuple[int, DatasetProfileView])
                | 'Group and merge' >> beam.CombinePerKey(CombineProfiledRows())
                .with_output_types(Tuple[int, bytes])
            )

        elif known_args.method == '3':
            # Method 3
            # Just a normal group by and reduce. Feeding each element into  our combiner
            # Works, but very slow: https://console.cloud.google.com/dataflow/jobs/us-central1/2022-10-06_21_50_33-1263227828518909216;step=Profile;bottomTab=WORKER_LOGS;logsSeverity=INFO;graphView=0?project=whylogs-359820
            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                .with_output_types(Dict[str, Any])
                | 'Add keys' >> beam.Map(lambda row: (to_day_start_millis(row['time']), row))
                .with_output_types(Tuple[int,  Dict[str, Any]])
                | 'Profile' >> beam.CombinePerKey(WhylogsCombineSingle())
                .with_output_types(Tuple[int, bytes])
            )

        elif known_args.method == 'window-1':
            # 43 minutes with the 4gig set: https://console.cloud.google.com/dataflow/jobs/us-west1/2022-10-08_21_40_49-127391940800366041;graphView=0?project=whylogs-359820
            window_size = timedelta(days=1).total_seconds()

            def timestamp(row: Dict[str, Any]):
                ts = to_day_start_millis(row['time'])
                return TimestampedValue((ts, row), ts)

            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                | 'Daily windows' >> beam.WindowInto(FixedWindows(window_size),
                                                     trigger=Repeatedly(
                                                         AfterAny(AfterCount(1000), AfterProcessingTime(30))),
                                                     accumulation_mode=AccumulationMode.DISCARDING)
                .with_output_types(Dict[str, Any])
                | 'TimestampedValue' >> beam.Map(timestamp)
                .with_output_types(Tuple[int, Tuple[str, Any]])
                | 'Merge' >> beam.CombinePerKey(WhylogsCombineSingle())
                .with_output_types(Tuple[int, bytes])
                # | beam.Map(lambda it: print(it))
            )

        elif known_args.method == 'window-batched':
            window_size = timedelta(days=1).total_seconds()

            def timestamp(row: Dict[str, Any]):
                ts = to_day_start_millis(row['time'])
                return TimestampedValue((ts, row), ts)

            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                | 'Daily windows' >> beam.WindowInto(FixedWindows(window_size),
                                                     trigger=Repeatedly(
                                                         AfterAny(AfterCount(1000), AfterProcessingTime(30))),
                                                     accumulation_mode=AccumulationMode.DISCARDING)
                .with_output_types(Dict[str, Any])
                | 'TimestampedValue' >> beam.Map(timestamp)
                .with_output_types(Tuple[int, Tuple[str, Any]])
                | 'Group into batches' >> beam.GroupIntoBatches(1000, max_buffering_duration_secs=35)
                .with_output_types(Tuple[int,  List[Dict[str, Any]]])
                | 'Profile' >> beam.CombinePerKey(WhylogsCombineBulk())
                .with_output_types(Tuple[int, bytes])
            )

        elif known_args.method == 'noop':
            hacker_news_data = (
                p
                | 'ReadTable' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                .with_output_types(Dict[str, Any])
                | 'Add keys' >> beam.Map(lambda row: (to_day_start_millis(row['time']), row))
                .with_output_types(Tuple[int,  Dict[str, Any]])
                | 'Group into batches' >> beam.GroupIntoBatches(10_000, max_buffering_duration_secs=30)
                .with_output_types(Tuple[int,  List[Dict[str, Any]]])
                | 'Profile' >> beam.CombinePerKey(NoOpCombiner())
                .with_output_types(Tuple[int, int])
            )

        elif known_args.method == 'counts':
            # just get counts for each day
            hacker_news_data = (
                p
                | 'Read data' >> beam.io.ReadFromBigQuery(table=table_spec, use_standard_sql=True)
                | 'Add keys' >> beam.Map(lambda row: (to_day_start_millis(row['time']), row))
                | 'Get count' >> beam.combiners.Count.PerKey()
            )

        else:
            raise Exception('Specify a number for which method to run')

        # output = hacker_news_data | 'Format' >> beam.Map(lambda x: f'{x}')
        hacker_news_data | 'Write' >> WriteToText(known_args.output)


def test():
    from datetime import datetime

    def to_day_start(milli_time: int) -> datetime:
        date = datetime.fromtimestamp(milli_time)
        return date.replace(second=0, microsecond=0, minute=0, hour=0)

    def to_day_start_millis(milli_time: Optional[int]) -> Optional[int]:
        if milli_time is None:
            return 0
        date = to_day_start(milli_time)
        return int(date.timestamp())

    print(to_day_start_millis(1665114166))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.DEBUG)
    run()
    # test()
