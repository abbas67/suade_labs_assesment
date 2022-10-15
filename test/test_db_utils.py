import datetime
import os
import pandas as pd
import unittest

from sqlalchemy.engine import create_engine

from suadelabsassessment.db_utils import execute_query, GET_ITEMS_SOLD, GET_ORDER_TOTAL_AVERAGE, GET_AVERAGE_DISCOUNT_RATE,\
    GET_TOTAL_DISCOUNT_AMOUNT, GET_TOTAL_CUSTOMERS


class TestDbUtils(unittest.TestCase):
    MOCK_DATABASE = os.path.join(os.path.dirname(__file__), 'resources', 'mock_db.db')

    def setUp(self):
        self.mock_engine = create_engine(f"sqlite:///{self.MOCK_DATABASE}")
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
                           ]).to_sql('orders', con=self.mock_engine, if_exists="replace")

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

                     ]).to_sql('order_lines', con=self.mock_engine, if_exists="replace")

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
                     ]).to_sql('commissions', con=self.mock_engine, if_exists="replace")

    def test_get_items_sold(self):
        with self.mock_engine.connect() as conn:
            self.assertEqual(7, execute_query(conn, GET_ITEMS_SOLD['sql'], date=datetime.date(2022, 10, 1)))

    def test_get_order_total_average(self):
        with self.mock_engine.connect() as conn:
            self.assertEqual(175, execute_query(conn, GET_ORDER_TOTAL_AVERAGE['sql'], date=datetime.date(2022, 10, 1)))

    def test_get_average_discount_rate(self):
        with self.mock_engine.connect() as conn:
            self.assertEqual(1.00, execute_query(conn, GET_AVERAGE_DISCOUNT_RATE['sql'], date=datetime.date(2022, 10, 1)))

    def test_get_total_discount_amount(self):
        with self.mock_engine.connect() as conn:
            self.assertEqual(7.00, execute_query(conn, GET_TOTAL_DISCOUNT_AMOUNT['sql'], date=datetime.date(2022, 10, 1)))

    def test_get_total_customers(self):
        with self.mock_engine.connect() as conn:
            self.assertEqual(3, execute_query(conn, GET_TOTAL_CUSTOMERS['sql'], date=datetime.date(2022, 10, 1)))


if __name__ == '__main__':
    unittest.main()
