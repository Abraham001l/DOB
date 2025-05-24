from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os


load_dotenv()
sql_user = os.getenv('SQL_USER')
sql_password = os.getenv('SQL_PASSWORD')
db_name = os.getenv('DB_NAME')



# Replace with your actual credentials and database name
engine = create_engine(f"postgresql://{sql_user}:{sql_password}@localhost:5432/{db_name}")

df = pd.read_sql_table('voo_base', con=engine)
print(df)