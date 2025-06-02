from sqlalchemy import create_engine
import pandas as pd
from dotenv import load_dotenv
import os
import yfinance as yf

import yfinance as yf
import pandas as pd

data = yf.download("ASST", period="max", interval="1h", auto_adjust=False)
data.reset_index(inplace=True)  # Make Date into a column
data.columns = data.columns.get_level_values(0)  # Removes multi-header structure

# Now 'Date' is in US Central Time
print(data.iloc[3225:3275])
print(data.iloc[-1])
