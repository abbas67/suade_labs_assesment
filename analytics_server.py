import json

from flask import Flask, abort, request
import logging

from suadelabsassessment.db_utils import new_engine
from suadelabsassessment.utils import validate_date, run_analytics

_logger = logging.getLogger(__name__)
engine = new_engine()
app = Flask(__name__)


@app.route("/api/v1/analytics", methods=['GET'])
def get_analytics():
    """
    Main endpoint.
    :return: Returns simplified analytics report.
    """
    _logger.info("Request received, validating...")
    date = validate_date(request.args.get("date"))
    if not date:
        abort(400)
    _logger.info(f"Date verified and valid: {date}")
    analytics_results = run_analytics(date, engine)
    _logger.info("Sending response.")
    return json.dumps(analytics_results)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)

