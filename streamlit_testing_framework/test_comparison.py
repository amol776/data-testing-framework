import pandas as pd
from data_processor import perform_comparison

def test_data_comparison():
    # Load test data
    source_df = pd.read_csv('test_data/source.csv')
    target_df = pd.read_csv('test_data/target.csv')
    
    # Define column mapping
    mapping = {
        'name': 'full_name',
        'salary': 'annual_salary'
    }
    
    # Define ignored columns
    ignored_columns = []
    
    try:
        # Perform comparison
        results = perform_comparison(source_df, target_df, mapping, ignored_columns)
        print("Comparison completed successfully!")
        print("\nResults:")
        for key, value in results.items():
            print(f"\n{key}:")
            print(f"Passed: {value['passed']}")
            print("Details:", value['details'])
    except Exception as e:
        print(f"Error during comparison: {str(e)}")

if __name__ == "__main__":
    test_data_comparison()
