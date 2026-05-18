import pandas as pd
import logging
import re
import numpy as np
logger = logging.getLogger(__name__)
placeholder_value=np.nan


def apply_window_filter(df, window_size, statistic, condition):
    """
    Applies a rolling window filter on the DataFrame and evaluates the condition.
    Rows with insufficient data points for the window size will be marked as False.
    """
    field, operator, value = parse_condition(condition)
    
    if field not in df.columns:
        logger.error(f"Field '{field}' not found in DataFrame.")
        return pd.Series([False] * len(df), index=df.index)

    if statistic == "mean":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).mean()
    elif statistic == "sum":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).sum()
    elif statistic == "min":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).min()
    elif statistic == "max":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).max()
    elif statistic == "std":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).std()
    else:
        logger.error(f"Unsupported statistic '{statistic}' for window filtering.")
        return pd.Series([False] * len(df), index=df.index)

    return eval_condition_on_series(windowed_series, operator, value)


def apply_window_filter_if_else_with_nan(df, window_size, statistic, condition, action_field, placeholder_value=np.nan):
    """
    Applies a rolling window filter for IF-THEN-ELSE but replaces failing values with NaN instead of removing rows.
    """
    field, operator, value = parse_condition(condition)

    if field not in df.columns:
        logger.error(f"Field '{field}' not found in DataFrame.")
        return df.copy(), pd.Series([False] * len(df))

    # Apply rolling window calculations
    if statistic == "mean":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).mean()
    elif statistic == "sum":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).sum()
    elif statistic == "min":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).min()
    elif statistic == "max":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).max()
    elif statistic == "std":
        windowed_series = df[field].rolling(window=window_size, min_periods=window_size).std()
    else:
        logger.error(f"Unsupported statistic '{statistic}' for window filtering.")
        return df.copy(), pd.Series([False] * len(df))

    # Evaluate the condition
    matches = eval_condition_on_series(windowed_series, operator, value)

    # Track NaNs due to insufficient window size
    is_window_nan = windowed_series.isna()

    # Modify only the affected column
    updated_df = df.copy()
    updated_df.loc[~matches, action_field] = placeholder_value

    return updated_df, is_window_nan




def eval_condition_on_series_with_nan(series, operator, value):
    """Evaluates the condition for a given series and operator.
    For values that are NaN (due to insufficient window data), returns None.
    """
    # Convert value to float for comparison
    try:
        value = float(value)
    except ValueError:
        return pd.Series([None] * len(series), index=series.index)

    # Create a result series initialized with None
    result = pd.Series([None] * len(series), index=series.index)

    # Comparison map
    comparison_map = {
        '>=': series >= value,
        '>': series > value,
        '<=': series <= value,
        '<': series < value,
        '!=': series != value,
        '=': series == value,
    }

    # Apply the condition only to non-NaN values
    condition_result = comparison_map.get(operator)
    result.loc[series.notna()] = condition_result.loc[series.notna()]

    return result


def eval_condition_on_series(series, operator, value):
    """Evaluates the condition for a given series and operator."""
    value = float(value)
    comparison_map = {
        '>=': series >= value,
        '>': series > value,
        '<=': series < value,
        '<': series < value,
        '!=': series != value,
        '=': series == value,
    }
    return comparison_map.get(operator)

def parse_condition(condition):
    """Parse a condition string into field, operator, and value."""
    operators = ['>=', '>', '<=', '<', '!=', '=']
    for op in operators:
        if f" {op} " in condition:
            field, value = re.split(rf'\s{op}\s', condition, maxsplit=1)
            return field.strip(), op, value.strip()
    raise ValueError(f"Invalid condition: {condition}")
