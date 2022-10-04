"""Run Apache Beam batch job to read csv file from Google Cloud Storage and sum transactions each
day.

Do python transform_transactions.py to run."""

from typing import NamedTuple
from datetime import datetime
import json
from apache_beam import PTransform, PCollection, Map, Filter, GroupBy, Pipeline
from apache_beam.io import ReadFromText, WriteToText


class Transaction(NamedTuple):
    """NamedTuple of parsed transaction.csv rows."""

    timestamp: datetime
    origin: str
    destination: str
    transaction_amount: float


class TransformTransactions(PTransform):
    """Parse lines of the csv input file.
    Filter by transaction_amount and date.
    Sum transaction amount each day.
    Return json syntax strings."""

    def expand(self, input_or_inputs: PCollection) -> PCollection:

        return (
            input_or_inputs
            | Map(self.parse_csv_row)
            | Filter(lambda x: x.transaction_amount > 20)
            | Filter(lambda x: x.timestamp.year >= 2010)
            | GroupBy(date=lambda x: x.timestamp.date()).aggregate_field(
                "transaction_amount", sum, "total_amount"
            )
            | Map(self.result_to_json)
        )

    @staticmethod
    def parse_csv_row(
        row: str, date_format: str = "%Y-%m-%d %H:%M:%S UTC"
    ) -> Transaction:
        """Parse row of csv input file to Transaction type."""
        timestamp, origin, destination, amount = row.split(",")
        return Transaction(
            datetime.strptime(timestamp, date_format),
            origin,
            destination,
            float(amount),
        )

    @staticmethod
    def result_to_json(element: NamedTuple) -> str:
        """Return elements of a NamedTuple in a json syntax sting."""
        return json.dumps(element._asdict(), default=str)


def run(
    input_: str = (
        r"gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
    ),
    output_dir: str = r"./output/",
    output_file: str = "results.jsonl.gz",
) -> None:
    """Read csv file, and write sum of transaction amount each day to gzipped jsonlines file.
    Args:
      input_: the path to the input file
      output_dir: the directory to write files to. The directory should already exist.
      output_file: the name of the output file.
    Returns:
      None.
    """
    with Pipeline() as pipeline:

        # pylint: disable=expression-not-assigned
        pipeline | "Read" >> ReadFromText(
            input_, skip_header_lines=1
        ) | "TransformTransactions" >> TransformTransactions() | "Write" >> WriteToText(
            output_dir,
            file_name_suffix=output_file,
            shard_name_template="",
            compression_type="gzip",
        )


if __name__ == "__main__":
    run()
