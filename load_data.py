"""
Basic module to load data from csv files and dump into sql database.
"""
import os
import pandas as pd
from sqlalchemy import MetaData, Table, Column, Integer, String, Date, Float, inspect

from suadelabsassessment.db_utils import new_engine, DATABASE, DATABASE_FILES

engine = new_engine()


def create_tables():
    """
    Creates tables using sqlalchemy MetaData class.
    :return: N/A
    """

    meta = MetaData()
    Table(
        'commissions', meta,
        Column('date', Date),
        Column('vendor_id', Integer),
        Column('rate', Float),
    )

    Table(
        'order_lines', meta,
        Column('order_id', Integer),
        Column('product_id', Integer),
        Column('product_description', String),
        Column('product_price', Integer),
        Column('product_vat_rate', Float),
        Column('discount_rate', Float),
        Column('quantity', Integer),
        Column('full_price_amount', Integer),
        Column('discounted_amount', Float),
        Column('vat_amount', Float),
        Column('total_amount', Float)
    )

    Table(
        'orders', meta,
        Column('id', Integer, primary_key=True),
        Column('created_at', Date),
        Column('vendor_id', Integer),
        Column('customer_id', Integer)
    )

    Table(
        'product_promotions', meta,
        Column('date', Date),
        Column('product_id', Integer),
        Column('promotion_id', Integer)
    )

    Table(
        'products', meta,
        Column('id', Integer, primary_key=True),
        Column('product_description', String),
    )

    Table(
        'promotions', meta,
        Column('id', Integer, primary_key=True),
        Column('description', String),
    )

    meta.create_all(engine)


def insert_into_tables():
    """
    Pandas is heavy and ideally wouldn't use it but for the sake of time...
    :return: N/A
    """
    engine_inspection = inspect(engine)

    for table_name in engine_inspection.get_table_names():
        temp_df = pd.read_csv(os.path.join(DATABASE_FILES, f'{table_name}.csv'))
        temp_df.to_sql(table_name, engine, if_exists='replace')


if __name__ == '__main__':
    create_tables()
    insert_into_tables()
