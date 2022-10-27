import argparse
import logging
import random
from math import ceil
from typing import Any, Dict, Iterator, List

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.typehints.batch import BatchConverter, ListBatchConverter

from apache_beam.options.value_provider import StaticValueProvider, RuntimeValueProvider


class TemplateArguments(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--output',
            dest='output',
            help='Output file or gs:// path to write results to.')
        parser.add_value_provider_argument(
            '--input_table',
            dest='input_table',
            help='Fully qualified big query table to read from. Set this or input_query')
        parser.add_value_provider_argument(
            '--input_query',
            dest='input_query',
            help='BigQuery SQL query to use as input. Set this or input_table.')


def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions()
    template_arguments = pipeline_options.view_as(TemplateArguments)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    if template_arguments.input_table is not None:
        input = {'table': template_arguments.input_table}
    elif template_arguments.input_query is not None:
        input = {'query': template_arguments.input_query,
                 'use_standard_sql': True}
    else:
        raise "Must supply either input_table or input_query."

    with beam.Pipeline(options=pipeline_options) as p:
        import pandas as pd
        import whylogs as why
        from whylogs.core import DatasetProfile, DatasetProfileView

        # Apparently adds considerable overhead
        logger = logging.getLogger()

        table_spec = bigquery.TableReference(
            projectId='whylogs-359820',
            datasetId='hacker_news',
            # tableId='comments'
            tableId='short'
            # tableId='comments_half'
        )
        query = 'select * from whylogs-359820.hacker_news.comments order by time'
        crypto_table = bigquery.TableReference(
            projectId='whylogs-359820',
            datasetId='btc_cash',
            tableId='transactions'
        )

        class WhylogsProfileMerger(beam.CombineFn):

            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile().view()

            def add_input(self, accumulator: DatasetProfileView, input: DatasetProfileView) -> DatasetProfileView:
                return accumulator.merge(input)

            def add_inputs(self, mutable_accumulator: DatasetProfileView, elements: List[DatasetProfileView]) -> DatasetProfileView:
                view = mutable_accumulator
                for current_view in elements:
                    view = view.merge(current_view)
                return view

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                view: DatasetProfileView = DatasetProfile().view()
                for current_view in accumulators:
                    view = view.merge(current_view)
                return view

            def extract_output(self, accumulator: DatasetProfileView) -> bytes:
                ser = accumulator.serialize()
                return ser

        class DatasetProfileBatchConverter(ListBatchConverter):
            def estimate_byte_size(self, batch):
                # TODO might be optional, according to the design doc
                nsampled = (
                    ceil(len(batch) * self.SAMPLE_FRACTION)
                    if len(batch) < self.SAMPLED_BATCH_SIZE else self.MAX_SAMPLES)
                mean_byte_size = sum(
                    len(element.serialize())
                    for element in random.sample(batch, nsampled)) / nsampled
                return ceil(mean_byte_size * len(batch))

        BatchConverter.register(DatasetProfileBatchConverter)

        class ProfileDoFn(beam.DoFn):
            def process_batch(self, batch: List[Dict[str, Any]]) -> Iterator[List[DatasetProfileView]]:
                logger.debug(f"Processing batch of size {len(batch)}")
                profile = DatasetProfile()
                profile.track(pd.DataFrame.from_dict(batch))
                yield [profile.view()]

        result = (
            p
            | 'ReadTable' >> beam.io.ReadFromBigQuery(**input)
            .with_output_types(Dict[str, Any])
            | 'Profile' >> beam.ParDo(ProfileDoFn())
            | 'Merge profiles' >> beam.CombineGlobally(WhylogsProfileMerger())
        )

        result | 'Write' >> WriteToText(template_arguments.output)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
