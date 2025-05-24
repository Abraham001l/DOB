import yfinance as yf

class KNN_DataManager:
    def __init__(self):
        pass

    def update_base(self):
        pass
    


ticker = "VOO"
data = yf.download(ticker, period="max", interval="1d", auto_adjust=False)
print(data)
data.reset_index(inplace=True)
print(data)

data.columns = data.columns.get_level_values(0) # Removes multi-header structure
data.to_csv("testo", index=False)
# 77.45279693603516
# 328.2378845214844