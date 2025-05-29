import yfinance as yf
from dotenv import load_dotenv
import os
import psycopg
import pandas as pd
from sqlalchemy import create_engine
import numpy as np

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

    # Update VOO_BASE
    def update_base(self):
        # Get num rows
        self.cur.execute("""SELECT COUNT(*) FROM VOO_BASE;""")
        n_rows = self.cur.fetchone()[0]

        # Check is base is empty
        if n_rows == 0: 
            self.instantiate_base()
        else:
            # Download data
            data = yf.download(self.ticker, period="max", interval="1d", auto_adjust=False)
            data.reset_index(inplace=True) # Make Date into a column
            data.columns = data.columns.get_level_values(0) # Removes multi-header structure

            # Getting more recent date of data thats in the table
            self.cur.execute("""SELECT Date FROM VOO_BASE ORDER BY Date DESC LIMIT 1""")
            most_recent_date = self.cur.fetchone()[0]
            
            # Collecting new rows
            new_rows = []
            for index, row in data.iloc[::-1].iterrows():
                if row['Date'] != most_recent_date:
                    new_rows.append(row)
                else: break
            
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
    
    # Update VOO_KNN
    def update_features(self):
        # Turning VOO_BASE into a pandas DataFrame 
        engine = create_engine(f"postgresql://{self.sql_user}:{self.sql_password}@localhost:5432/{self.db_name}")
        data = pd.read_sql_table('voo_base', con=engine)

        # Percent breakout predicted
        prcnt_breakout = .01

        # ---------- Creating Features ----------
        data['return'] = data['adj_close'].pct_change()
        data.dropna(inplace=True)

        # MACD (Percentage)
        ema_12 = data['adj_close'].ewm(span=12, adjust=False).mean()
        ema_26 = data['adj_close'].ewm(span=26, adjust=False).mean()
        data.loc[:, 'macd_percent'] = 100 * (ema_12 - ema_26) / ema_12

        # Percentage distance from 200-day MA
        ma_200 = data['adj_close'].rolling(window=200).mean()
        data.loc[:, 'pct_distance_200ma'] = 100 * (data['adj_close'] - ma_200) / ma_200

        # Volume Ratio
        data.loc[:, 'volume_ratio'] = data['volume'] / data['volume'].rolling(window=20).mean()

        # RSI
        data.loc[:, 'rsi'] = 100 - (100 / (1 + (data['return'].rolling(window=14).mean() / data['return'].rolling(window=14).std())))

        # Volatility (Standard Deviation of Returns over a 20-day window)
        data.loc[:, 'volatility'] = data['return'].rolling(window=20).std() * np.sqrt(252)  # Annualized volatility
        # Drop rows with NaN values created by rolling calculations
        data.dropna(inplace=True)

        # Adding Breakout Labels
        data.loc[:, 'breakout'] = (data['adj_close'].shift(-5) >= (data['adj_close'] * (1+prcnt_breakout))).astype(int)
        data.dropna(inplace=True)
        data.reset_index(drop=True, inplace=True)

        # Store DataFrame into SQL table
        data.to_sql(
            'voo_knn',                # Table name
            engine,                   # SQLAlchemy engine
            if_exists='replace',      # Options: 'fail', 'replace', 'append'
            index=False               # Do not write DataFrame index as a column
        )
    
    # Getting the Featured Data
    def get_featured_data(self):
        # Turning VOO_KNN into a pandas DataFrame
        engine = create_engine(f"postgresql://{self.sql_user}:{self.sql_password}@localhost:5432/{self.db_name}")
        data = pd.read_sql_table('voo_knn', con=engine)
        return data

