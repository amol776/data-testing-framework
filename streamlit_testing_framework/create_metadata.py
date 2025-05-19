import pandas as pd

# Create sample metadata
data = {
    'ComparisonType': ['Feed To Feed', 'Feed To Feed', 'Feed To Feed'],
    'Filename1': ['test_data/source.csv', 'test_data/skip_this.csv', 'test_data/source2.csv'],
    'Filename2': ['test_data/target.csv', 'test_data/skip_that.csv', 'test_data/target2.csv'],
    'Separator1': [',', ',', '|'],
    'Separator2': [',', ',', '|'],
    'ColumnMapping': ['name:full_name,salary:annual_salary', 'col1:col2', 'old_name:new_name'],
    'IgnoredColumns': ['', '', ''],
    'SkipFile': ['', '#Skip this comparison', '']
}

# Create DataFrame
df = pd.DataFrame(data)

# Save to Excel
df.to_excel('test_data/metadata.xlsx', index=False)
print("Metadata Excel file created successfully!")
