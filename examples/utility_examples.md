## Function Examples

Below are examples demonstrating the use and output of the core `PolarsPulse` functions.

### Setup

```python
import polars as pl
import datetime
from polarspulse import (
    column_type_ident,
    column_missing_prop,
    row_missing_prop,
    column_dup_ind,
    row_dup_ind,
    num_stats,
    num_outlier_stats,
    cat_stats
)

# Sample DataFrame for most examples
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5, 5], # Row 5 is duplicate of 4 based on id & val
    "group": ["A", "B", "A", "C", None, "C"], # Null, C=2, A=2, B=1
    "value": [10.0, 15.5, 10.0, 99.9, 20.1, 20.1], # Outlier?
    "flag": [True, False, True, False, True, True], # Boolean
    "constant": ["X", "X", "X", "X", "X", "X"], # Zero variance
    "date": pl.date_range(datetime.date(2025, 1, 1), datetime.date(2025, 1, 6), interval="1d", eager=True)
})

# DataFrame with specific issues for certain examples
df_issues = pl.DataFrame({
    "col_a": [1, 2, 3, 1, 2, 3],
    "col_b": [1, 2, 3, 1, 2, 3], # Duplicate of col_a
    "col_c": [None, 5.0, float('nan'), float('inf'), -float('inf'), 6.0]
})
```

## Column Type Identification
```python
# Use thresholds: max 3 unique values OR <50% unique for categorical
col_types = column_type_ident(df, unique_n_threshold=3, unique_prop_threshold=0.5)
print(col_types.select(["column", "col_dtype", "approx_n_unique", "col_class"]))
```
```plaintext
shape: (6, 4)
┌──────────┬───────────┬─────────────────┬───────────┐
│ column   ┆ col_dtype ┆ approx_n_unique ┆ col_class │
│ ---      ┆ ---       ┆ ---             ┆ ---       │
│ str      ┆ str       ┆ u32             ┆ str       │
╞══════════╪═══════════╪═════════════════╪═══════════╡
│ id       ┆ Int64     ┆ 5               ┆ num       │ # 5 unique values > 3 threshold, and col dtype is Int
│ group    ┆ String    ┆ 4               ┆ other     │ # 4 unique values > 3 threshold, but col dtype is Str
│ value    ┆ Float64   ┆ 4               ┆ num       │ # 4 unique values > 3 threshold, and col dtype is Float
│ flag     ┆ Boolean   ┆ 2               ┆ cat       │ # 2 unique values <= 3 threshold, and col dtype is Bool
│ constant ┆ String    ┆ 1               ┆ zero_var  │ # 1 unique value = zero variance (constant), independent of threshold and col dtype
│ date     ┆ Date      ┆ 6               ┆ time      │ # col dtype is Date, independent of threshold
└──────────┴───────────┴─────────────────┴───────────┘
```
