"""
Utils function for neatness.
"""
import logging
from datetime import datetime

from suadelabsassessment.db_utils import QUERIES, execute_query

_logger = logging.getLogger(__name__)


def run_analytics(date, dao):
    """
    Executes a list of queries
    :param dao:
    :param date:
    :return: Analytics report.
    """
    analytics = {}
    _logger.info("Running analytics...")
    with dao.connect() as conn:
        for analytic in QUERIES:
            sql = analytic['sql']
            identifier = analytic['identifier']
            analytics[identifier] = execute_query(conn, sql, date=date)

    _logger.info("Analytics complete.")
    analytics['commissions'] = {}

    return analytics


def validate_date(date_string):
    """
    :param date_string:
    :return: converted date object.
    :raises: ValueError should be caught if raised by datetime.datetime.
    """
    if isinstance(date_string, str):
        try:
            converted_date = datetime.fromisoformat(date_string).date()
        except ValueError:
            _logger.error("Error converting string to datetime object.")
            return

        if converted_date > datetime.today().date():
            _logger.error(f"{converted_date} is in the future, aborting.")
            return
        else:
            return converted_date


