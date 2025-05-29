from data_manager import KNN_DataManager
from sklearn.neighbors import KNeighborsClassifier
import pickle
import sys
import pandas as pd
import os
import matplotlib.pyplot as plt

class KNN_Executor():
    def __init__(self):
        self.data_manager = KNN_DataManager()
        self.update_data()

    def __del__(self):
        del self.data_manager

    # Getting the Featured Data
    def update_data(self):
        self.data_manager.update_base()
        self.data_manager.update_features()
        self.ft_data = self.data_manager.get_featured_data()
    
    # Load Model
    def load_model(self, model_filename):
        cur_dir = os.getcwd()
        model_filename = os.path.join(cur_dir, 'KNN_VOO\\Models', model_filename)
        self.model = pickle.load(open(model_filename, 'rb'))
        
    # Train Model
    def train(self, model_filename, start_date, end_date):
        # Making datetime objs for start and end dates
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)

        # Croping data to window of interest
        start_i = -1
        end_i = -1
        for index, row in self.ft_data.iloc[::-1].iterrows():
            if (row['date'] <= start_date and start_i == -1):
                start_i = index
            if (row['date'] <= end_date and end_i == -1):
                end_i = index
        self.ft_data_cropped = self.ft_data.iloc[start_i:end_i+1]


        # Prepare features and labels
        features = self.ft_data_cropped[['macd_percent', 'pct_distance_200ma', 'volume_ratio', 'rsi', 'volatility']]
        labels = self.ft_data_cropped['breakout']

        # Training KNN model
        k = 3
        knn = KNeighborsClassifier(n_neighbors=k)
        knn.fit(features, labels)

        # Saving KNN model
        cur_dir = os.getcwd()
        model_filename = os.path.join(cur_dir, 'KNN_VOO\\Models', model_filename)
        with open(model_filename, 'wb') as file:
            pickle.dump(knn, file)
        print(f"Model saved to {model_filename}")
    
    # Train Model
    def run_model(self, date):
        # Converting date to pandas Timestamp
        date = pd.Timestamp(date)

        # Running Model
        row_index = self.ft_data.index[self.ft_data['date'] == date][0] # Gives (index, type) which is why [0]
        inputs = pd.DataFrame([self.ft_data.loc[row_index, :][['macd_percent', 'pct_distance_200ma', 'volume_ratio', 'rsi', 'volatility']]])
        prediction = self.model.predict(inputs)[0]
        invest = prediction == 1
        return invest

    # Simulate Model
    def simulate(self, start_date, end_date):
        # Making datetime objs for start and end dates
        start_date = pd.Timestamp(start_date)
        end_date = pd.Timestamp(end_date)

        # Croping data to window of interest
        start_i = -1
        end_i = -1
        for index, row in self.ft_data.iloc[::-1].iterrows():
            if (row['date'] <= start_date and start_i == -1):
                start_i = index
            if (row['date'] <= end_date and end_i == -1):
                end_i = index
        ft_data_cropped = self.ft_data.iloc[start_i:end_i+1]

        # Setup variables
        balance = 100
        balances = []
        invested = False
        days_invested = 0
        entry_price = 0

        print(ft_data_cropped.iloc[len(ft_data_cropped['adj_close'])-1]['adj_close']/ft_data_cropped.iloc[0]['adj_close'])

        # Simulating
        for index, row in ft_data_cropped.iterrows():
            if not invested:
                # Run model
                invested = self.run_model(row['date'])
                entry_price = row['adj_close']
            else:
                days_invested += 1
                if days_invested == 5:
                    invested = False
                    days_invested = 0
                    balance *= row['adj_close']/entry_price
            balances.append(balance)
        
        plt.plot([i for i in range(len(balances))],balances)
        plt.show()







knn_executor = KNN_Executor()
knn_executor.train('VOO_2020-01-01_2024-01-01.pkl', '2020-01-01', '2024-01-01')
knn_executor.load_model('VOO_2020-01-01_2024-01-01.pkl')
knn_executor.simulate('2024-01-02', '2024-12-30')
del knn_executor

# 2019-12-31 00:00:00
# 2019-12-31 00:00:00