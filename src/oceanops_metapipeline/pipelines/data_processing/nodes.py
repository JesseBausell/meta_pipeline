"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.3
"""

import pandas as pd
import requests
from typing import Dict

def json_pandas(oops_platforms_region: requests.models.Response) -> pd.DataFrame:
    oops_platforms_region = oops_platforms_region.json()
    platforms_df = pd.DataFrame(oops_platforms_region['data'])
    return platforms_df