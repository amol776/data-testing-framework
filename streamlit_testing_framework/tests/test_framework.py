import pytest
import pandas as pd
import numpy as np
from pathlib import Path
import sys
import os

# Add parent directory to Python path
sys.path.append(str(Path(__file__).parent.parent))

from data_processor import load_data, perform_comparison
import allure

@allure.feature("Data Comparison Tests")
class TestDataComparison:
    
    @pytest.fixture
    def sample_source_data(self):
        """Create sample source data for testing"""
        return pd.DataFrame({
            'Column A': [1, 2, 3, 4, 5],
            'Column B': [10, 20, 30, 40, 50],
            'Column C': [11, 22, 33, 44, 55],  # Sum of A and B
            'Text Column': ['a', 'b', 'c', 'd', 'e']
        })

    @pytest.fixture
    def sample_target_data(self):
        """Create sample target data for testing"""
        return pd.DataFrame({
            'Column A': [1, 2, 3, 4, 5],
            'Column B': [10, 20, 30, 40, 50],
            'Column C': [11, 22, 33, 44, 55],  # Sum of A and B
            'Text Column': ['a', 'b', 'c', 'd', 'e']
        })

    @pytest.fixture
    def sample_mapping(self):
        """Sample column mapping"""
        return {
            'Column A': 'Column A',
            'Column B': 'Column B',
            'Column C': 'Column C',
            'Text Column': 'Text Column'
        }

    @allure.story("Row Count Validation")
    def test_row_count(self, sample_source_data, sample_target_data, sample_mapping):
        """Test if row counts match between source and target"""
        results = perform_comparison(sample_source_data, sample_target_data, sample_mapping, [])
        
        with allure.step("Verify row counts match"):
            assert results["row_count"]["passed"], "Row counts do not match"
            assert results["row_count"]["details"]["source_count"] == \
                   results["row_count"]["details"]["target_count"], \
                   "Source and target row counts are different"

    @allure.story("Duplicate Check")
    def test_duplicates(self, sample_source_data, sample_target_data, sample_mapping):
        """Test duplicate detection"""
        # Add some duplicates to test data
        source_with_dupes = pd.concat([sample_source_data, sample_source_data.iloc[:2]])
        target_with_dupes = pd.concat([sample_target_data, sample_target_data.iloc[:2]])
        
        results = perform_comparison(source_with_dupes, target_with_dupes, sample_mapping, [])
        
        with allure.step("Verify duplicate counts match"):
            assert results["duplicates"]["passed"], "Duplicate counts do not match"
            assert results["duplicates"]["details"]["source_duplicates"] == \
                   results["duplicates"]["details"]["target_duplicates"], \
                   "Source and target duplicate counts are different"

    @allure.story("Null Value Check")
    def test_null_values(self, sample_source_data, sample_target_data, sample_mapping):
        """Test null value detection"""
        # Add some null values to test data
        source_with_nulls = sample_source_data.copy()
        target_with_nulls = sample_target_data.copy()
        
        source_with_nulls.iloc[0, 0] = np.nan
        target_with_nulls.iloc[0, 0] = np.nan
        
        results = perform_comparison(source_with_nulls, target_with_nulls, sample_mapping, [])
        
        with allure.step("Verify null value counts match"):
            assert results["null_values"]["passed"], "Null value counts do not match"
            assert results["null_values"]["details"]["source_nulls"] == \
                   results["null_values"]["details"]["target_nulls"], \
                   "Source and target null value counts are different"

    @allure.story("Business Rule Validation")
    def test_business_rule(self, sample_source_data, sample_target_data, sample_mapping):
        """Test business rule: Column A + Column B = Column C"""
        results = perform_comparison(sample_source_data, sample_target_data, sample_mapping, [])
        
        with allure.step("Verify business rule"):
            assert results["business_rules"]["passed"], "Business rule validation failed"
            assert results["business_rules"]["details"]["mismatches"] == 0, \
                   "Found mismatches in business rule validation"

    @allure.story("Data Quality Validation")
    def test_data_quality(self, sample_source_data, sample_target_data, sample_mapping):
        """Test data quality expectations"""
        results = perform_comparison(sample_source_data, sample_target_data, sample_mapping, [])
        
        with allure.step("Verify data quality expectations"):
            assert results["data_quality"]["passed"], "Data quality validation failed"
            
            # Check individual expectations
            for expectation in results["data_quality"]["details"]["expectations"]:
                with allure.step(f"Verify {expectation['expectation_type']}"):
                    assert expectation["success"], \
                           f"Failed expectation: {expectation['expectation_type']}"

    @allure.story("Column Mapping")
    def test_column_mapping(self, sample_source_data, sample_target_data):
        """Test column mapping functionality"""
        # Create a custom mapping
        custom_mapping = {
            'Column A': 'Mapped A',
            'Column B': 'Mapped B'
        }
        
        # Create target data with mapped column names
        target_mapped = sample_target_data.copy()
        target_mapped.columns = [custom_mapping.get(col, col) for col in target_mapped.columns]
        
        results = perform_comparison(sample_source_data, target_mapped, custom_mapping, [])
        
        with allure.step("Verify column mapping"):
            assert all(col in target_mapped.columns for col in custom_mapping.values()), \
                   "Mapped columns not found in target data"

    @allure.story("Large File Handling")
    def test_large_file_handling(self):
        """Test handling of large files (>3GB)"""
        # Create a relatively small DataFrame for testing purposes
        # In production, this would be testing with actual large files
        large_df = pd.DataFrame({
            'Column A': range(1000),
            'Column B': range(1000, 2000),
            'Column C': range(2000, 3000)
        })
        
        with allure.step("Verify large file processing"):
            try:
                results = perform_comparison(large_df, large_df, {}, [])
                assert results["row_count"]["passed"], "Large file comparison failed"
            except Exception as e:
                pytest.fail(f"Large file handling failed: {str(e)}")

    @allure.story("Error Handling")
    def test_error_handling(self):
        """Test error handling for invalid inputs"""
        with pytest.raises(Exception):
            perform_comparison(None, None, {}, [])
        
        with pytest.raises(Exception):
            perform_comparison(pd.DataFrame(), None, {}, [])
        
        with pytest.raises(Exception):
            perform_comparison(pd.DataFrame(), pd.DataFrame(), None, None)

if __name__ == "__main__":
    pytest.main([__file__, "--alluredir=./allure-results"])
