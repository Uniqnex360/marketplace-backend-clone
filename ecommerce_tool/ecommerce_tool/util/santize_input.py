import pandas as pd

def sanitize_value(value, default=None, value_type=None):
    # Handle nested dict
    if isinstance(value, dict):
        return {
            k: sanitize_value(v, default=default, value_type=value_type)
            for k, v in value.items()
        }

    # Handle nested list
    if isinstance(value, list):
        return [
            sanitize_value(v, default=default, value_type=value_type)
            for v in value
        ]

    # Handle None and NaN from pandas or numpy
    if value is None or pd.isna(value):
        if default is not None:
            return default
        if value_type == float:
            return 0.0
        elif value_type == int:
            return 0
        elif value_type == str:
            return ""
        else:
            return None

    # Handle "nan" or "NaN" as string
    if isinstance(value, str) and value.strip().lower() == 'nan':
        if value_type == float:
            return 0.0
        elif value_type == int:
            return 0
        else:
            return ""

    # Attempt type conversion
    try:
        if value_type == float:
            return float(value)
        elif value_type == int:
            return int(float(value))
        elif value_type == str:
            return str(value)
    except (ValueError, TypeError):
        if value_type == float:
            return 0.0
        elif value_type == int:
            return 0
        elif value_type == str:
            return ""

    return value
