import apache_beam as beam
from datetime import datetime as dt
from apache_beam.testing.util import equal_to
from apache_beam.testing.util import assert_that

transactions_20 = []


def get_transactions(row):
    """ gets transactions from pcollection that have a value greater than 20 and appends to pre-defined list

    Args:
        row (list)

    Returns:
        pcollections
    """

    if float(row[3]) > 20:
        transactions_20.append(row)
    return(row)


def filter_date(row):
    """ filters dates that are earlier than 2010

    Args:
        row (list)

    Returns:
        pcollection
    """
    d = row[0]
    d = dt.strptime(d.replace('UTC', '').strip(), '%Y-%m-%d %H:%M:%S')
    return d.year >= 2010


class compositeTransform(beam.PTransform):

    def expand(self, input_col):

        transform = (
            input_col

            | 'filter_date' >> beam.Filter(filter_date)
            | 'groupby_date_sum' >> beam.GroupBy(lambda d: d[0]).aggregate_field(lambda t: float(t[3]), sum, 'total')

        )
        return transform

# Chained Tranformations


pipeline = beam.Pipeline()


test_pipeline = (pipeline
                 | "read_from_text" >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
                 | "split_comma" >> beam.Map(lambda row: row.split(','))
                 | "print_transactions_greater_than_20" >> beam.Map(lambda row: get_transactions(row))
                 | "filter_date" >> beam.Filter(filter_date)
                 | "groupby_date_sum" >> beam.GroupBy(lambda d: d[0]).aggregate_field(lambda t: float(t[3]), sum, 'total')
                 | "write_to_json" >> beam.io.WriteToText('output/results.jsonl.gz'))


pipeline.run()

print("Here are all transactions where transaction_amount is greater than 20")
print(transactions_20)


# Composite Transformations

with beam.Pipeline() as pipeline_2:
    input_data = (pipeline_2
                  | "Read from text file" >> beam.io.ReadFromText('gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv', skip_header_lines=1)
                    | 'split_comma' >> beam.Map(lambda row: row.split(',')))

    # unit test
    # check if input data containts the following elements
    assert_that(input_data, equal_to([['2009-01-09 02:54:25 UTC', 'wallet00000e719adfeaa64b5a', 'wallet00001866cb7e0f09a890', '1021101.99'],
                                      ['2017-01-01 04:22:23 UTC', 'wallet00000e719adfeaa64b5a',
                                       'wallet00001e494c12b3083634', '19.95'],
                                      ['2017-03-18 14:09:16 UTC', 'wallet00001866cb7e0f09a890',
                                       'wallet00001e494c12b3083634', '2102.22'],
                                      ['2017-03-18 14:10:44 UTC', 'wallet00001866cb7e0f09a890',
                                       'wallet00000e719adfeaa64b5a', '1.00030'],
                                      ['2017-08-31 17:00:09 UTC', 'wallet00001e494c12b3083634',
                                       'wallet00005f83196ec58e4ffe', '13700000023.08'],
                                      ['2018-02-27 16:04:11 UTC', 'wallet00005f83196ec58e4ffe', 'wallet00001866cb7e0f09a890', '129.12']]))

    transform_export = (input_data
                        | "composite_transform" >> compositeTransform()
                        | "write_to_json" >> beam.io.WriteToText('output/results_2.jsonl.gz'))
