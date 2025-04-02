import re

import pandas as pd


class DataTransformer:
    def __init__(self, data):
        self.data = data

    def transform_claim_data(self, table_config):
        date_columns = ["date_of_service", "claim_date", "last_modified_date"]
        self.convert_data_types(table_config)
        self.drop_duplicates()
        self.add_index()
        self.convert_date_columns(date_columns)
        self.apply_claim_business_rules()
        self.convert_date_columns_to_str(date_columns)
        self.keep_rows_with_lowest_nulls(table_config["primary_key"])
        self.remove_duplicate_rows_keep_highest_value(table_config["primary_key"])
        self.drop_index()
        return self.data

    def transform_patient_data(self, table_config):
        date_columns = ["date_of_birth", "last_modified_date", "date_of_residence"]
        self.convert_data_types(table_config)
        self.drop_duplicates()
        self.add_index()
        self.convert_date_columns(date_columns)
        self.apply_patient_business_rules()
        self.convert_date_columns_to_str(date_columns)
        self.keep_rows_with_lowest_nulls(table_config["primary_key"])
        self.remove_duplicate_rows_keep_highest_value(table_config["primary_key"])
        self.drop_index()
        return self.data

    def transform_provider_data(self, table_config):
        date_columns = ["start_date"]
        self.convert_data_types(table_config)
        self.drop_duplicates()
        self.add_index()
        self.convert_date_columns(date_columns)
        self.apply_provider_business_rules()
        self.convert_date_columns_to_str(date_columns)
        self.keep_rows_with_lowest_nulls(table_config["primary_key"])
        self.remove_duplicate_rows_keep_highest_value(table_config["primary_key"])
        self.drop_index()
        return self.data

    def convert_data_types(self, table_config):
        self.data = self.data.astype(table_config["data_type"])

    def drop_duplicates(self):
        self.data = self.data.drop_duplicates()

    def add_index(self):
        self.data["Index"] = range(len(self.data))

    def drop_index(self):
        self.data.drop("Index", axis=1, inplace=True)

    def convert_date_columns(self, date_columns):
        for col in date_columns:
            self.data[col] = pd.to_datetime(self.data[col], errors='coerce')

    def apply_claim_business_rules(self):
        # Define functions for business rules (claim_id, visit_type, patient_id)
        def business_rule_claim_id(s):
            if s.startswith("CF#") and s.endswith("NN"):
                return s
            else:
                return None

        def check_keywords(description):
            keywords = ["physical", "routine", "illness", "surgery"]
            if description is not None and any(
                keyword in description.lower() for keyword in keywords
            ):
                return description.upper()
            else:
                return None

        def transform_type(value):
            try:
                float_val = float(value)
                int_val = int(float_val)
                allowed_values = {100, 50, 0}
                if int_val in allowed_values:
                    return str(int_val)
                else:
                    return None
            except (ValueError, TypeError):
                return None  # Handle non-numeric values or conversion errors

        def patient_id_check(s):
            if s.endswith("U"):
                return s
            else:
                return None

        def convert_to_float(s):
            try:
                return float(s)
            except (ValueError, TypeError):
                return None

        # Apply these functions to the respective columns
        self.data["total_cost"] = self.data["total_cost"].apply(convert_to_float)
        self.data["claim_id"] = self.data["claim_id"].apply(business_rule_claim_id)
        self.data["visit_type"] = self.data["visit_type"].apply(check_keywords)
        self.data["patient_id"] = self.data["patient_id"].apply(patient_id_check)
        self.data["coverage_type"] = self.data["coverage_type"].apply(transform_type)

    def apply_patient_business_rules(self):
        def patient_id_check(s):
            if s.endswith("U"):
                return s
            else:
                return None

        def valid_zipcode(zipcode):
            # Define a regular expression pattern for valid ZIP codes
            zipcode_pattern = r"^\d{5}(-\d{4})?$"

            # Remove any trailing '.0' from the string
            cleaned_zipcode = re.sub(r"\.0$", "", zipcode)

            if re.match(zipcode_pattern, cleaned_zipcode):
                return cleaned_zipcode
            else:
                return None

        self.data["patient_id"] = self.data["patient_id"].apply(patient_id_check)
        self.data["zipcode"] = self.data["zipcode"].apply(valid_zipcode)

    def apply_provider_business_rules(self):
        def provider_id_check(s):
            if s.endswith("A") and s.startswith("N"):
                return s
            else:
                return None

        def valid_zipcode(zipcode):
            # Define a regular expression pattern for valid ZIP codes
            zipcode_pattern = r"^\d{5}(-\d{4})?$"

            if re.match(zipcode_pattern, zipcode):
                return zipcode
            else:
                return None

        def check_keywords(description):
            keywords = [
                "family_physician",
                "orthopedist",
                "heart_surgeon",
                "physical_therapist",
            ]

            if description is not None and any(
                keyword in description.lower() for keyword in keywords
            ):
                return description.upper()
            else:
                return None

        self.data["provider_id"] = self.data["provider_id"].apply(provider_id_check)
        self.data["zipcode"] = self.data["zipcode"].apply(valid_zipcode)
        self.data["specialty_code"] = self.data["specialty_code"].apply(check_keywords)

    def keep_rows_with_lowest_nulls(self, primary_key_column):
        # Step 1: Count the number of null values in each row
        self.data["null_count"] = self.data.isnull().sum(axis=1)

        # Step 2: Identify rows with the lowest null counts within each primary key group
        self.data["min_null_count"] = self.data.groupby(primary_key_column)[
            "null_count"
        ].transform(min)
        rows_to_keep = self.data["null_count"] == self.data["min_null_count"]

        # Step 3: Keep rows with the lowest null counts within each group
        self.data = self.data[rows_to_keep].drop(
            columns=["null_count", "min_null_count"]
        )

    def remove_duplicate_rows_keep_highest_value(self, primary_key_column):
        # Step 1: Sort the DataFrame by the unique value column in descending order
        df_sorted = self.data.sort_values(by="Index", ascending=False)

        # Step 2: Remove duplicate key rows while keeping the first occurrence (highest value)
        df_no_duplicates = df_sorted.drop_duplicates(
            subset=primary_key_column, keep="first"
        )

        # Step 3: Revert the DataFrame to its original order
        self.data = df_no_duplicates.sort_index(ascending=True)

    def convert_date_columns_to_str(self, date_columns):
        for col in date_columns:
            self.data[col] = self.data[col].astype(str)
