import logging
import re
import random
from math import ceil
from typing import Any, Dict, Iterator, List
from whylogs.core import DatasetProfile, DatasetProfileView

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.typehints.batch import BatchConverter, ListBatchConverter

from apache_beam.options.value_provider import StaticValueProvider, RuntimeValueProvider

# matches PROJECT:DATASET.TABLE.
table_ref_regex = re.compile(r'[^:\.]+:[^:\.]+\.[^:\.]+')

class TemplateArguments(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--output',
            dest='output',
            required=True,
            help='Output file or gs:// path to write results to.')
        parser.add_value_provider_argument(
            '--input',
            required=True,
            default='',
            dest='input',
            help='This can be a SQL query that includes a table name or a fully qualified reference to a table with the form PROJECT:DATASET.TABLE')
        parser.add_value_provider_argument(
            '--log_level',
            dest='log_level',
            default='INFO',
            help='One of the logging levels from the logging module as a string.')

def or_default(value: RuntimeValueProvider):
    if value.is_accessible():
        return value.get()
    else:
        return value.default_value



def run(argv=None, save_main_session=True):
    pipeline_options = PipelineOptions()
    template_arguments = pipeline_options.view_as(TemplateArguments)
    pipeline_options.view_as(
        SetupOptions).save_main_session = save_main_session

    logging.getLogger().setLevel(logging.getLevelName(or_default(template_arguments.log_level)))
    logger = logging.getLogger()

    input = or_default(template_arguments.input)
    if table_ref_regex.match(input):
        pipeline_input = {'table': input}
    else:
        logger.info("Assuming input was a query becausee it didn't have the form PROJECT:DATASET.TABLE")
        pipeline_input = {'query': input , 'use_standard_sql': True}

    if template_arguments.output.is_accessible():
        output = template_arguments.output.get()
    else:
        raise "Missing output argument"

    with beam.Pipeline(options=pipeline_options) as p:
        import pandas as pd
        from whylogs.core import DatasetProfile, DatasetProfileView


        class WhylogsProfileMerger(beam.CombineFn):

            def create_accumulator(self) -> DatasetProfileView:
                return DatasetProfile().view()

            def add_input(self, accumulator: DatasetProfileView, input: DatasetProfileView) -> DatasetProfileView:
                return accumulator.merge(input)

            def add_inputs(self, mutable_accumulator: DatasetProfileView, elements: List[DatasetProfileView]) -> DatasetProfileView:
                view = mutable_accumulator
                logger.debug("adding %s inputs", len(elements))
                for current_view in elements:
                    view = view.merge(current_view)
                return view

            def merge_accumulators(self, accumulators: List[DatasetProfileView]) -> DatasetProfileView:
                view: DatasetProfileView = DatasetProfile().view()
                logger.debug("merging %s views", len(accumulators))
                for current_view in accumulators:
                    view = view.merge(current_view)
                return view

            def extract_output(self, accumulator: DatasetProfileView) -> bytes:
                return accumulator.serialize()

        class DatasetProfileBatchConverter(ListBatchConverter):
            def estimate_byte_size(self, batch):
                # TODO might be optional, according to the design doc
                # Was copied from the implementation of ListBatchConverter to make it work with profiles.
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
                logger.debug("Processing batch of size %s", len(batch))
                profile = DatasetProfile()
                profile.track(pd.DataFrame.from_dict(batch))
                yield [profile.view()]

        result = (
            p
            | 'ReadTable' >> beam.io.ReadFromBigQuery(**pipeline_input)
            .with_output_types(Dict[str, Any])
            | 'Profile' >> beam.ParDo(ProfileDoFn())
            | 'Merge profiles' >> beam.CombineGlobally(WhylogsProfileMerger())
        )

        result | 'Write' >> WriteToText(output)


if __name__ == '__main__':
    run()
