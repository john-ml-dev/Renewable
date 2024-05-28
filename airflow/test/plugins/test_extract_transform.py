import unittest
import pandas as pd
from utils import extract_transform
class Extract_transform(unittest.TestCase):
    def test_extract(self):
        output_data = extract_transform("airflow/Renewable") 
        assert isinstance(output_data, pd.DataFrame)
    
    def test_transform(self):
        pass
    

if __name__ == '__main__':
    unittest.main()