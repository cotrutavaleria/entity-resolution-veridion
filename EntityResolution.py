from fuzzywuzzy import fuzz
import pandas as pd
from rapidfuzz import process, fuzz


class EntityResolution:
    def __init__(self, preprocessed_dataset):
        self.preprocessed_dataset = preprocessed_dataset

    def find_duplicates(self, website_domain, company_name, threshold=85):
        duplicate_groups = []
        primary_key_name = 'primary_column'
        self.preprocessed_dataset[primary_key_name] = self.preprocessed_dataset.apply(
            lambda row: row[website_domain] if pd.notna(row[website_domain]) else row[company_name], axis=1
        )

        company_list = self.preprocessed_dataset[primary_key_name].tolist()
        while company_list:
            if not company_list:
                break

            current_company = company_list.pop(0)

            matches = process.extract(current_company, company_list, scorer=fuzz.ratio, limit=None)
            duplicates = [match[0] for match in matches if match[1] >= threshold]

            group = self.preprocessed_dataset[
                self.preprocessed_dataset[primary_key_name].isin(duplicates + [current_company])]
            duplicate_groups.append(group)

            self.preprocessed_dataset = self.preprocessed_dataset.drop(group.index)
            company_list = self.preprocessed_dataset[primary_key_name].tolist()
            print(len(company_list))
        return duplicate_groups