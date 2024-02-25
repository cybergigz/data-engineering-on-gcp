import configparser

import pandas as pd
import sqlalchemy
from sqlalchemy import text


CONFIG_FILE = "local.conf"

parser = configparser.ConfigParser()
parser.read(CONFIG_FILE)

database = parser.get("mysql_config", "database")
user = parser.get("mysql_config", "username")
password = parser.get("mysql_config", "password")
host = parser.get("mysql_config", "host")
port = parser.get("mysql_config", "port")

uri = f"mysql+pymysql://{user}:{password}@{host}/{database}"
engine = sqlalchemy.create_engine(uri)

sql = """
        CREATE TABLE IF NOT EXISTS titanic (
            passenger_id           INT,
            survived               INT,
            pclass                 INT,
            name                   VARCHAR(300)
        )
    """
    
# for k, v in tables.items():
with engine.connect() as conn:
     conn.execute(text(f"DROP TABLE IF EXISTS Titanic"))
     conn.execute(text(sql))
     print("Created Titanic table successfully")

     df = pd.read_csv("titanic.csv")
     df.to_sql("titanic", con=uri, if_exists="replace", index=False)
     print(f"Imported titanic data successfully")
    