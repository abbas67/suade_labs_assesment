"""
DB Util module for neatness.
"""
import logging
import os

from sqlalchemy import create_engine, text

_logger = logging.getLogger(__name__)
DATABASE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'db', 'suade_db.db')
DATABASE_FILES = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data', 'files')

GET_ITEMS_SOLD = {
    "sql": """
    SELECT SUM(ol.quantity) 
    FROM orders o
    INNER JOIN order_lines ol
    ON o.id = ol.order_id  
    WHERE DATE(created_at)=:date""",
    "identifier": "items"
}

GET_ORDER_TOTAL_AVERAGE = {
    "sql": """
    SELECT AVG(ol.total_amount)
    FROM orders o
    INNER JOIN order_lines ol
    ON o.id = ol.order_id  
    WHERE DATE(created_at)=:date""",
    "identifier": "order_total_avg"
}

GET_AVERAGE_DISCOUNT_RATE = {
    "sql": """
    SELECT AVG(ol.discount_rate)
    FROM orders o
    INNER JOIN order_lines ol
    ON o.id = ol.order_id  
    WHERE DATE(created_at)=:date
    """,
    "identifier": "discount_range_average"
}

GET_TOTAL_CUSTOMERS ={
    "sql": """
    SELECT COUNT(DISTINCT customer_id)
    FROM orders 
    WHERE DATE(created_at)=:date
    """,
    "identifier": "customers"
}

GET_TOTAL_DISCOUNT_AMOUNT = {
    "sql": """
    SELECT SUM(ol.discounted_amount)
    FROM orders o
    INNER JOIN order_lines ol
    ON o.id = ol.order_id  
    WHERE DATE(created_at)=:date
    """,
    "identifier": "total_discount_amount"
}


QUERIES = [GET_TOTAL_DISCOUNT_AMOUNT, GET_TOTAL_CUSTOMERS, GET_AVERAGE_DISCOUNT_RATE, GET_ORDER_TOTAL_AVERAGE, GET_ITEMS_SOLD]


def new_engine():
    """
    Creates a basic sqlalchemy engine object.
    :return: new engine object.
    """
    _logger.info("Generating new engine.")
    return create_engine(f"sqlite:///{DATABASE}")


def execute_query(conn, sql, **query_params):
    """
    Executes query, all have homogenous inputs and outptus.
    :param conn:
    :param sql:
    :param query_params:
    :return: results of query.
    """
    _logger.info(f"Executing Query: {sql}")
    return conn.execute(text(sql), query_params).fetchone()[0]
