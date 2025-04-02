import pandas as pd


class DataExtractor:
    def __init__(self, csv_file_path):
        self.csv_file_path = csv_file_path

    def extract_data(self):
        return pd.read_csv(self.csv_file_path)
