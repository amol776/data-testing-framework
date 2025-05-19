import pandas as pd
from typing import List, Dict, Tuple
import os

class MetadataProcessor:
    def __init__(self, metadata_file: str):
        """Initialize the metadata processor with an Excel file"""
        self.metadata_df = pd.read_excel(metadata_file)
        
    def get_comparison_pairs(self) -> List[Dict]:
        """
        Process metadata file and return list of comparison configurations
        Returns list of dictionaries containing comparison settings
        """
        comparison_pairs = []
        
        for _, row in self.metadata_df.iterrows():
            # Skip if marked with #
            if str(row.get('SkipFile', '')).strip().startswith('#'):
                continue
                
            comparison_type = row.get('ComparisonType', '')
            if comparison_type == 'Feed To Feed':
                pair = {
                    'source_file': str(row.get('Filename1', '')),
                    'target_file': str(row.get('Filename2', '')),
                    'comparison_type': comparison_type,
                    'source_type': 'CSV file',  # Default to CSV
                    'target_type': 'CSV file',
                    'source_separator': row.get('Separator1', ','),
                    'target_separator': row.get('Separator2', ','),
                    'mapping': self._get_column_mapping(row),
                    'ignored_columns': self._get_ignored_columns(row)
                }
                comparison_pairs.append(pair)
                
        return comparison_pairs
    
    def _get_column_mapping(self, row: pd.Series) -> Dict[str, str]:
        """Extract column mapping from metadata row"""
        mapping = {}
        mapping_str = row.get('ColumnMapping', '')
        if pd.notna(mapping_str) and mapping_str:
            try:
                # Expected format: "col1:mapped_col1,col2:mapped_col2"
                pairs = mapping_str.split(',')
                for pair in pairs:
                    source, target = pair.split(':')
                    mapping[source.strip()] = target.strip()
            except:
                pass
        return mapping
    
    def _get_ignored_columns(self, row: pd.Series) -> List[str]:
        """Extract ignored columns from metadata row"""
        ignored = []
        ignored_str = row.get('IgnoredColumns', '')
        if pd.notna(ignored_str) and ignored_str:
            try:
                ignored = [col.strip() for col in ignored_str.split(',')]
            except:
                pass
        return ignored

def process_metadata_comparisons(metadata_file: str) -> List[Dict]:
    """
    Process metadata file and return comparison results
    Returns list of dictionaries containing comparison results
    """
    processor = MetadataProcessor(metadata_file)
    return processor.get_comparison_pairs()
