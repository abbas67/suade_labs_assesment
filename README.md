# suade_labs_assesment

**Setup**
Uses Python 3.7

1. Install requirements in requirements.txt.
2. run load_data.py to populate the database.

**Example Request**
http://localhost:3000/api/v1/analytics?date=2019-08-09

Note: The date format should be in ISO format or will be rejected.

**Main things I would do differently with more time.**

1. More test cases with a wider range of data.
2. Granular and isolated tests for EVERY function
3. Use a full on ORM approach rather than using hardcoded sql strings.
4. Probably avoid using pandas  if possible.
5. Add the commission data to the endpoint,seems like it would have involved adding another table to the join, then multiplying rates and total amounts.
6. Add tests for load_data.py
7. In a real life environment, wouldn't use sqllite.