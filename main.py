from etl import config
from etl.data_extractor import DataExtractor
from etl.data_loader import DataLoader
from etl.data_transformer import DataTransformer


def process_data(file_path_list, table_config):
    for file_path in file_path_list:
        extractor = DataExtractor(file_path)
        transformer = DataTransformer(extractor.extract_data())

        if table_config["main_table"] == "claim":
            transformer.transform_claim_data(table_config)
        elif table_config["main_table"] == "patient":
            transformer.transform_patient_data(table_config)
        elif table_config["main_table"] == "provider":
            transformer.transform_provider_data(table_config)

        handler = DataLoader(transformer.data, "astellas.db")
        handler.load_processed_data(table_config)


def main():
    claim_file_path = [
        "data/raw_data/claims_priming.csv",
        "data/raw_data/claims_update.csv",
    ]
    patient_file_path = [
        "data/raw_data/patients_priming.csv",
        "data/raw_data/patients_update.csv",
    ]
    provider_file_path = [
        "data/raw_data/providers_priming.csv",
        "data/raw_data/providers_update.csv",
    ]

    process_data(claim_file_path, config.CLAIM_TABLE)
    process_data(patient_file_path, config.PATIENT_TABLE)
    process_data(provider_file_path, config.PROVIDER_TABLE)


if __name__ == "__main__":
    main()
