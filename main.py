from EntityResolution import EntityResolution
from Preprocessing import Preprocessing
import pandas as pd

def preprocess(input_file_path):
    preprocessing = Preprocessing(input_file_path)
    preprocessing.load_dataset()
    preprocessing.preprocess_dataset()
    dataset = preprocessing.get_preprocessed_dataset()
    return dataset

def get_duplicate_groups(dataset):
    entity_resolution = EntityResolution(dataset)
    return entity_resolution.find_duplicates('website_domain', 'company_name')


def write_duplicates(duplicate_groups, output_file_path):
    duplicates_df = pd.concat(duplicate_groups, ignore_index=True)
    duplicates_df.to_parquet(output_file_path, engine='pyarrow', index=False)
    print(f"Deduplication complete. Output saved in {output_file_path}")

def main():
    input_file_path = "dataset/veridion_entity_resolution_challenge.snappy.parquet"
    output_file_path = 'dataset/output/entity_resolution_solution.parquet'

    dataset = preprocess(input_file_path)
    duplicate_groups = get_duplicate_groups(dataset)
    write_duplicates(duplicate_groups, output_file_path)

if __name__ == "__main__":
    main()
