from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os


# load_dotenv()
# sql_user = os.getenv('SQL_USER')
# sql_password = os.getenv('SQL_PASSWORD')
# db_name = os.getenv('DB_NAME')



# # Replace with your actual credentials and database name
# engine = create_engine(f"postgresql://{sql_user}:{sql_password}@localhost:5432/{db_name}")

# df = pd.read_sql_table('voo_base', con=engine)
# print(df)

# # 91.4081268310547
# # 91.4081268310547





def calculate_rsi(prices, period=14):
    """
    Calculates the Relative Strength Index (RSI).

    Args:
        prices (pd.Series): A pandas Series of closing prices.
        period (int): The period over which to calculate RSI.

    Returns:
        pd.Series: A pandas Series of RSI values.
    """

    delta = prices.diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    return rsi

# Example usage:
data = {'Close': [10, 12, 15, 13, 16, 18, 17, 20, 22, 21, 23, 25, 24, 26, 28]}
df = pd.DataFrame(data)
rsi_values = calculate_rsi(df['Close'])
print(rsi_values)
