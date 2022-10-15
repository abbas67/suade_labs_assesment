import json
import os
import unittest
import datetime
from unittest import mock

import pandas as pd
from freezegun import freeze_time
from sqlalchemy import create_engine


from analytics_server import app, run_analytics

app.testing = True


class TestAnalyticsServer(unittest.TestCase):

    # iso format is universal and easy to deal with.
    VALID_DATE = datetime.date(2022, 10, 1).isoformat()
    FUTURE_DATE = datetime.date(2099, 1, 1).isoformat()

    MOCK_DATABASE = os.path.join(os.path.dirname(__file__), 'resources', 'mock_db.db')

    def setup_mock_engine(self):

        mock_engine = create_engine(f"sqlite:///{self.MOCK_DATABASE}")
        pd.DataFrame(columns=['id',
                              'created_at',
                              'vendor_id',
                              'customer_id'],
                     data=[
                         [1, datetime.datetime(2022, 10, 1, 10, 0, 0), 101, 1],
                         [2, datetime.datetime(2022, 10, 1, 11, 0, 0), 102, 2],
                         [3, datetime.datetime(2022, 10, 1, 12, 0, 0), 103, 3],
                         [4, datetime.datetime(2022, 10, 1, 13, 0, 0), 104, 1],
                         [5, datetime.datetime(2022, 11, 1, 10, 0, 0), 101, 1],
                         [6, datetime.datetime(2022, 11, 1, 11, 0, 0), 102, 2],
                         [7, datetime.datetime(2022, 11, 1, 12, 0, 0), 103, 3],
                         [8, datetime.datetime(2022, 11, 1, 13, 0, 0), 104, 1]
                           ]).to_sql('orders', con=mock_engine, if_exists="replace")

        pd.DataFrame(columns=['order_id',
                              'product_id',
                              'product_description',
                              'product_price',
                              'product_vat_rate',
                              'discount_rate',
                              'quantity',
                              'full_price_amount',
                              'discounted_amount',
                              'vat_amount',
                              'total_amount'],
                     data=[
                         [1, 1, 'product_1', 100, 1.00, 1.00, 1, 100, 1.00, 1.00, 100],
                         [2, 2, 'product_2', 100, 1.00, 1.00, 2, 200, 2.00, 2.00, 200],
                         [3, 3, 'product_3', 100, 1.00, 1.00, 3, 300, 3.00, 3.00, 300],
                         [1, 1, 'product_1', 100, 1.00, 1.00, 1, 100, 1.00, 1.00, 100],
                         [5, 1, 'product_1', 100, 1.00, 1.00, 1, 100, 1.00, 1.00, 100],
                         [6, 2, 'product_2', 100, 1.00, 1.00, 2, 200, 2.00, 2.00, 200],
                         [7, 3, 'product_3', 100, 1.00, 1.00, 3, 300, 3.00, 3.00, 300],
                         [8, 1, 'product_1', 100, 1.00, 1.00, 1, 100, 1.00, 1.00, 100],

                     ]).to_sql('order_lines', con=mock_engine, if_exists="replace")

        pd.DataFrame(columns=['date',
                              'vendor_id',
                              'rate'],
                     data=[
                         [datetime.datetime(2022, 10, 1), 101, 0.50],
                         [datetime.datetime(2022, 10, 1), 102, 0.50],
                         [datetime.datetime(2022, 10, 1), 103, 0.50],
                         [datetime.datetime(2022, 10, 1), 104, 0.50],
                         [datetime.datetime(2022, 11, 1), 101, 0.50],
                         [datetime.datetime(2022, 11, 1), 102, 0.50],
                         [datetime.datetime(2022, 11, 1), 103, 0.50],
                         [datetime.datetime(2022, 11, 1), 104, 0.50]
                     ]).to_sql('commissions', con=mock_engine, if_exists="replace")
        return mock_engine

    @mock.patch("analytics_server.run_analytics")
    def test_get_analytics_valid_request(self, mock_run_analytics):
        with app.test_client() as client:
            mock_analytics = {
                "total_discount_amount": "foo",
                "customers": 0,
                "discount_range_average": "foo",
                "order_total_avg": "foo",
                "items": "foo",
                "commissions": {}}

            mock_run_analytics.return_value = mock_analytics
            actual = client.get('/api/v1/analytics', query_string={"date": self.VALID_DATE})
            self.assertEqual(mock_analytics, json.loads(actual.data.decode()))

    def test_run_analytics_data_exists(self):

        expected = {'total_discount_amount': 7.0,
                    'customers': 3,
                    'discount_range_average': 1.0,
                    'order_total_avg': 175.0,
                    'items': 7,
                    'commissions': {}}
        self.assertEqual(expected, run_analytics(datetime.date(2022, 10, 1), self.setup_mock_engine()))

    def test_run_analytics_no_data(self):
        expected = {'commissions': {},
                    'customers': 0,
                    'discount_range_average': None,
                    'items': None,
                    'order_total_avg': None,
                    'total_discount_amount': None}
        self.assertEqual(expected, run_analytics(datetime.date(2022, 1, 1), self.setup_mock_engine()))

    @freeze_time("2022-10-15")
    def test_get_analytics_invalid_requests(self):

        with app.test_client() as client:

            response = client.get('/api/v1/analytics')
            self.assertEqual(response.status_code, 400, 'No date passed in should be a BAD REQUEST - 400')

            response = client.get('/api/v1/analytics', query_string={"date": self.FUTURE_DATE})
            self.assertEqual(response.status_code, 400, 'Future date passed in should be a BAD REQUEST - 400')

            response = client.get('/api/v1/analytics', query_string={"date": 20220101})
            self.assertEqual(response.status_code, 400, 'Integer passed in should be a BAD REQUEST - 400')

            response = client.get('/api/v1/analytics', query_string={"date": "2022-01-0111"})
            self.assertEqual(response.status_code, 400, 'Invalid date string passed in should be a BAD REQUEST - 400')


if __name__ == '__main__':
    unittest.main()
