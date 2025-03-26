# Entity Resolution @ Veridion 

## 1. Short Description:
The goal of this solution is to determine unique companies and to classify appropriately the duplicate records with
minor changes in the dataset.

## 2. Characteristics:
1. Detects duplicates using **Fuzzy Matching**: This method finds similar website domains and company names, even 
with little differences.
2. Website domain and company name prioritization: For increased accuracy, if there is any website domain provided, it 
is used as the primary identification, otherwise, the company name will be used.
3. Effective & scalable: PyArrow  library effectively manages big datasets by iterative deduplication, which eliminates
repetitive comparisons.
4. Customizable similarity threshold: For a smooth control, the fuzzy matching threshold can be modified, however, the
default value is 85%.
5. Optimized output: For effective analysis and storage, the output is saved in a parquet format.
## 3. Code explanation:
The solution uses an organized OOP design that includes data preprocessing, entity resolution and output production.

### Preprocessing:
Minor changes in company names and domain names, additional spaces, or different capitalizations are examples of 
discrepancies that may be present in raw data (for example **Owens Liquors Inc.** and **Owens Liquor Store**).
For a higher accuracy, company names and website domains are converted to lowercase and the whitespaces are removed to 
create a standard formatting. 
```
def preprocess_dataset(self):
    self.__dataset['company_name'] = self.__dataset['company_name'].str.lower().str.strip()
    self.__dataset['website_domain'] = self.__dataset['website_domain'].str.lower().str.strip()
```

### Entity Resolution:
Company names and website domains may have small variations (for example **Bello Skin Co™** and **Bello Skin Co.**). 
That is why, exact string matching would miss these small differences, instead, the fuzzy string matching is a better 
option.
1. Creating a primary column: For grouping the duplicates, a good approach is to create a new column, reflecting the 
primary key the comparisons are made on.
2. Use **RapidFuzz** (a quicker substitute for **FuzzyWuzzy**) to iterate through the dataset and compare each object with 
the others.
3. Compute similarity scores using **fuzz.ratio**, then create duplicate groups for entities that match the criterion 
**(≥threshold)**.
4. Append the duplicates to an array called **duplicate_groups** and remove them from the preprocessed dataset.
5. The last step is returning the **duplicate groups**.
```
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
```

### Saving output:
In the main script, there are several methods used to access the functionalities offered by **Preprocessing and 
EntityResolution classes**. Additionally, a method for saving the output is implemented. The output is saved in a 
parquet file. 

```
def write_duplicates(duplicate_groups, output_file_path):
    duplicates_df = pd.concat(duplicate_groups, ignore_index=True)
    duplicates_df.to_parquet(output_file_path, engine='pyarrow', index=False)
    print(f"Deduplication complete. Output saved in {output_file_path}")

```

### Potential improvements:
1. Consider additional columns (for instance, main_country, primary_phone, primary_email) for improved accuracy.
2. Replace the iterative deduplication with a parallel comparison for faster execution on large datasets as the 
dataset provided.

