import os
import pytest

import os, sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from sqlalchemy import create_engine, text
from tests import load_dotenv_simple


def test_opteryx_connection():
    load_dotenv_simple('.env')
    OPTERYX_CONNECTION_STRING = os.getenv("OPTERYX_CONNECTION_STRING")

    engine = create_engine(OPTERYX_CONNECTION_STRING)

    with engine.connect() as conn:
        result = conn.execute(text("SELECT id, name FROM $planets LIMIT 10"))
        for row in result:
            print(row)

if __name__ == "__main__":
    pytest.main([__file__])