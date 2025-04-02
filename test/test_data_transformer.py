import pandas as pd
import pytest

from etl.config import CLAIM_TABLE
from etl.data_transformer import DataTransformer

sample_data = {
    "claim_id": ["CF#1000078NN", "CF#67890NN", "CF#1000078NN", "CF#54321NN"],
    "patient_id": ["62000U", "P5678U", "62000U", "P1234U"],
    "provider_id": ["N3T5300A", "N3T5300A", "N3T5300A", "N3T5300A"],
    "visit_type": ["surgery", "routine", "surgery", "illness"],
    "total_cost": [782.08, 50.0, 782.08, 75.0],
    "coverage_type": [100, 50, 100, 0],
    "date_of_service": ["10/10/17", "2022-02-20", "10/10/17", "2022-03-25"],
    "claim_date": ["01/03/19", "2022-02-01", "01/03/19", "2022-03-01"],
    "billed": ["T", "F", "T", "T"],
    "last_modified_date": ["02/08/21", "2022-02-02", "02/08/21", "2022-03-02"],
}

sample_df = pd.DataFrame(sample_data)

transformer = DataTransformer(sample_df)


class TestDataTransformer:
    @pytest.fixture
    def transformed_data(self):
        return transformer.transform_claim_data(CLAIM_TABLE)

    def test_drop_duplicates(self, transformed_data):
        assert len(transformed_data) == len(transformed_data.drop_duplicates())

    def test_add_index(self, transformed_data):
        transformer.add_index()
        assert "Index" in transformed_data.columns

    def test_drop_index(self, transformed_data):
        assert "Index" not in transformed_data.columns

    def test_convert_date_columns(self, transformed_data):
        table_config = CLAIM_TABLE
        date_columns = table_config.get("date_columns", [])
        for col in date_columns:
            assert isinstance(transformed_data[col].dtype, pd.DatetimeTZDtype())


# To run the tests, use the following command:
# pytest -v test_data_transformer.py
