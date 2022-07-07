import pandas as pd
from typing import Dict


def string_to_datetime(string_cols: Dict[str, str], df: pd.DataFrame) -> pd.DataFrame:
    for col_name, col_format in string_cols.items():
        df[col_name] = pd.to_datetime(
            df[col_name],
            format=col_format,
        )
    return df
