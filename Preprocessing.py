import pyarrow.parquet as pq

class Preprocessing:
    def __init__(self, file_path):
        self.__dataset = None
        self.__file_path = file_path

    def load_dataset(self):
        self.__dataset = pq.read_table(self.__file_path).to_pandas()

    def preprocess_dataset(self):
        self.__dataset['company_name'] = self.__dataset['company_name'].str.lower().str.strip()
        self.__dataset['website_domain'] = self.__dataset['website_domain'].str.lower().str.strip()

    def get_preprocessed_dataset(self):
        return self.__dataset

