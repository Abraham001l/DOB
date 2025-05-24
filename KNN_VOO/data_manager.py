import yfinance as yf
from dotenv import load_dotenv
import os
import psycopg

class KNN_DataManager:
    def __init__(self):
        self.instantiate_env_vars()
        self.instantiate_cursor()
        self.ticker = 'VOO'

    def __del__(self):
        self.clean_up()

    # Instantiate ENV Variables
    def instantiate_env_vars(self):
        load_dotenv()
        self.sql_user = os.getenv('SQL_USER')
        self.sql_password = os.getenv('SQL_PASSWORD')
        self.db_name = os.getenv('DB_NAME')
    
    # Instantiate Cursor
    def instantiate_cursor(self):
        # Establish the connection
        self.conn = psycopg.connect(
            dbname=self.db_name,
            user=self.sql_user,
            password=self.sql_password,
            host='localhost',
            port=5432
        )

        # Create a cursor
        self.cur = self.conn.cursor()
    
    # Cleans Up SQL Connections
    def clean_up(self):
        self.cur.close()
        self.conn.close()

    # Instantiate VOO_BASE
    def instantiate_base(self):
        # Download data
        data = yf.download(self.ticker, period="max", interval="1d", auto_adjust=False)
        data.reset_index(inplace=True) # Make Date into a column
        data.columns = data.columns.get_level_values(0) # Removes multi-header structure
        
        # Loop through rows adding each one
        for index, row in data.iterrows():
            self.cur.execute(
                """INSERT INTO VOO_BASE (Date, Adj_Close, Close, High, Low, Open, Volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    row['Date'],
                    row['Adj Close'],
                    row['Close'],
                    row['High'],
                    row['Low'],
                    row['Open'],
                    row['Volume']
                )
            )

        # Commiting changes to table
        self.conn.commit()

    def update_base(self):
        # Get num rows
        self.cur.execute("""SELECT COUNT(*) FROM VOO_BASE;""")
        n_rows = self.cur.fetchone()[0]

        # Check is base is empty
        if n_rows == 0: 
            self.instantiate_base()
        else:
            print(1)
            # Download data
            data = yf.download(self.ticker, period="max", interval="1d", auto_adjust=False)
            data.reset_index(inplace=True) # Make Date into a column
            data.columns = data.columns.get_level_values(0) # Removes multi-header structure

            # Getting more recent date of data thats in the table
            self.cur.execute("""SELECT Date FROM VOO_BASE ORDER BY Date DESC LIMIT 1""")
            most_recent_date = self.cur.fetchone()[0]
            print(2,most_recent_date)
            # Collecting new rows
            new_rows = []
            for index, row in data.iloc[::-1].iterrows():
                if row['Date'] != most_recent_date:
                    new_rows.append(row)
                else: break
            
            print(3, len(new_rows))
            # Adding new rows to VOO_BASE
            for row in new_rows[::-1]:
                self.cur.execute(
                    """INSERT INTO VOO_BASE (Date, Adj_Close, Close, High, Low, Open, Volume)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (
                        row['Date'],
                        row['Adj Close'],
                        row['Close'],
                        row['High'],
                        row['Low'],
                        row['Open'],
                        row['Volume']
                    )
                )

            # Commiting changes to table
            self.conn.commit()

knn_manager = KNN_DataManager()
knn_manager.update_base()
del knn_manager