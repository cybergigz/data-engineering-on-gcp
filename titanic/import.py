import configparser

# import pandas as pd
import sqlalchemy
from sqlalchemy import text


CONFIG_FILE = "local.conf"

uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
engine = sqlalchemy.create_engine(uri)

sql = """
        CREATE TABLE IF NOT EXISTS titanic (
            passenger_id           INT,
            survived               INT,
            pclass                 INT,
            name                   VARCHAR(300)
        )
    ---"""
    
for k, v in tables.items():
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {k}"))
        conn.execute(text(v))
        print(f"Created {k} table successfully")