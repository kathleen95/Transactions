"""Unit test for TransformTransaction class.
Use python -m unittest to run.
"""
import unittest
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to
import apache_beam as beam
from transform_transactions import TransformTransactions


class TestTransformTransactions(unittest.TestCase):
    """Unit test for TransformTransaction class."""

    def test_transactions(self) -> None:
        """Run test case for TransactionTransformation class."""

        transactions = [
            "2010-01-01 12:30:25 UTC,a,topp,1000.0",
            "2022-01-02 13:55:09 UTC,BBBBBB,martha,1000.0",
            "2010-01-01 11:33:01 UTC,CCCC,CCCC,1000",
            "1995-09-10 05:23:00 UTC,martha,BBBBBB,1000",
            "2022-09-10 08:00:00 UTC,topp,a,20",
        ]

        expected_output = [
            '{"date": "2010-01-01", "total_amount": 2000.0}',
            '{"date": "2022-01-02", "total_amount": 1000.0}',
        ]

        with TestPipeline() as test_pipeline:
            output = test_pipeline | beam.Create(transactions) | TransformTransactions()

            assert_that(output, equal_to(expected_output))
