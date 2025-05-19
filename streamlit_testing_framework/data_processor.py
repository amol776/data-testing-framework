import pandas as pd
import dask.dataframe as dd
import great_expectations as ge
from typing import Dict, List, Optional, Tuple, Any
import zipfile
import io
import os
import sqlalchemy
import requests
from dotenv import load_dotenv

load_dotenv()

def load_data(source_type: str, input_obj: Any, separator: Optional[str] = None) -> pd.DataFrame:
    """
    Load data from various sources into a pandas DataFrame.
    Handles large files using Dask when necessary.
    """
    try:
        if source_type in ["CSV file", "Dat file"]:
            # For large files, use Dask
            if input_obj is not None:
                content = input_obj.read()
                if len(content) > 3 * 1024 * 1024 * 1024:  # 3GB
                    # Save to temporary file for Dask to process
                    temp_file = "temp_large_file.csv"
                    with open(temp_file, "wb") as f:
                        f.write(content)
                    ddf = dd.read_csv(temp_file, sep=separator or ",")
                    df = ddf.compute()
                    os.remove(temp_file)
                else:
                    # For smaller files, use pandas directly
                    df = pd.read_csv(io.BytesIO(content), sep=separator or ",")
                return df
            
        elif source_type == "Parquet file":
            if input_obj is not None:
                content = input_obj.read()
                return pd.read_parquet(io.BytesIO(content))
            
        elif source_type == "Flat files inside zipped folder":
            if input_obj is not None:
                z = zipfile.ZipFile(input_obj)
                dfs = []
                for filename in z.namelist():
                    if filename.endswith(('.txt', '.csv', '.dat')):
                        with z.open(filename) as f:
                            df = pd.read_csv(f, sep=separator or ",")
                            dfs.append(df)
                return pd.concat(dfs, ignore_index=True)
            
        elif source_type in ["SQL Server", "Teradata"]:
            # Get connection details from environment variables
            connection_string = os.getenv(f"{source_type.replace(' ', '_').upper()}_CONNECTION")
            if connection_string:
                engine = sqlalchemy.create_engine(connection_string)
                query = input_obj if isinstance(input_obj, str) else ""
                return pd.read_sql(query, engine)
            
        elif source_type == "Stored Procs":
            connection_string = os.getenv("SQL_SERVER_CONNECTION")
            if connection_string:
                engine = sqlalchemy.create_engine(connection_string)
                with engine.connect() as conn:
                    return pd.read_sql(f"EXEC {input_obj}", conn)
            
        elif source_type == "API":
            if isinstance(input_obj, str):
                response = requests.get(input_obj)
                response.raise_for_status()
                return pd.DataFrame(response.json())
            
        return pd.DataFrame()
    
    except Exception as e:
        raise Exception(f"Error loading data from {source_type}: {str(e)}")

def perform_comparison(
    source_df: pd.DataFrame,
    target_df: pd.DataFrame,
    mapping: Dict[str, str],
    ignored_columns: List[str]
) -> Dict[str, Any]:
    """
    Perform comprehensive comparison between source and target DataFrames.
    Includes data quality checks using Great Expectations.
    """
    try:
        results = {
            "row_count": {"passed": False, "details": {}},
            "duplicates": {"passed": False, "details": {}},
            "null_values": {"passed": False, "details": {}},
            "business_rules": {"passed": False, "details": {}},
            "column_mapping": {"passed": False, "details": {}},
            "data_quality": {"passed": False, "details": {}}
        }

        # Apply column mapping
        if mapping:
            source_df = source_df.rename(columns=mapping)

        # Remove ignored columns
        source_df = source_df.drop(columns=[col for col in ignored_columns if col in source_df.columns])
        target_df = target_df.drop(columns=[col for col in ignored_columns if col in target_df.columns])

        # 1. Row Count Check
        source_count = len(source_df)
        target_count = len(target_df)
        results["row_count"]["passed"] = source_count == target_count
        results["row_count"]["details"] = {
            "source_count": source_count,
            "target_count": target_count,
            "difference": abs(source_count - target_count)
        }

        # 2. Duplicate Check
        source_dupes = source_df.duplicated().sum()
        target_dupes = target_df.duplicated().sum()
        results["duplicates"]["passed"] = source_dupes == target_dupes
        results["duplicates"]["details"] = {
            "source_duplicates": source_dupes,
            "target_duplicates": target_dupes
        }

        # 3. Null Value Check
        source_nulls = source_df.isnull().sum().to_dict()
        target_nulls = target_df.isnull().sum().to_dict()
        results["null_values"]["passed"] = source_nulls == target_nulls
        results["null_values"]["details"] = {
            "source_nulls": source_nulls,
            "target_nulls": target_nulls
        }

        # 4. Business Rule Check (Column A + Column B = Column C)
        common_columns = set(source_df.columns) & set(target_df.columns)
        if all(col in common_columns for col in ["Column A", "Column B", "Column C"]):
            source_sum = source_df["Column A"] + source_df["Column B"]
            target_value = target_df["Column C"]
            rule_check = (source_sum == target_value).all()
            results["business_rules"]["passed"] = rule_check
            results["business_rules"]["details"] = {
                "rule_satisfied": rule_check,
                "mismatches": len(source_sum[source_sum != target_value])
            }

        # 5. Great Expectations Data Quality Checks
        ge_source = ge.from_pandas(source_df)
        ge_target = ge.from_pandas(target_df)

        # Basic expectations
        expectations = [
            ge_source.expect_table_row_count_to_be_between(min_value=1, max_value=None),
            ge_source.expect_table_columns_to_match_ordered_list(list(source_df.columns)),
            ge_target.expect_table_row_count_to_be_between(min_value=1, max_value=None),
            ge_target.expect_table_columns_to_match_ordered_list(list(target_df.columns))
        ]

        # Add column-specific expectations
        for column in common_columns:
            print(f"Processing column: {column}, dtype: {source_df[column].dtype}")
            try:
                if source_df[column].dtype in ['int64', 'float64']:
                    print(f"Adding numeric expectations for column: {column}")
                    expectations.extend([
                        ge_source.expect_column_values_to_be_of_type(column, "int64"),
                        ge_target.expect_column_values_to_be_of_type(column, "int64")
                    ])
                else:
                    print(f"Adding string expectations for column: {column}")
                    expectations.extend([
                        ge_source.expect_column_values_to_be_of_type(column, "object"),
                        ge_target.expect_column_values_to_be_of_type(column, "object")
                    ])
            except Exception as e:
                print(f"Error processing column {column}: {str(e)}")

        # Store expectation results
        results["data_quality"]["details"]["expectations"] = [
            {
                "expectation_type": exp.expectation_config.expectation_type,
                "success": exp.success,
                "result": exp.result
            }
            for exp in expectations
        ]
        results["data_quality"]["passed"] = all(exp.success for exp in expectations)

        return results

    except Exception as e:
        raise Exception(f"Error during comparison: {str(e)}")
