## Function Examples

Below are examples demonstrating the use and output of the core `PolarsPulse` functions.

### Setup

```python
import polars as pl
# import datetime
from datetime import date
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
    "group": ["A", "B", "A", None, "C",  "C"], # Null, C=2, A=2, B=1
    "value": [10.0, 15.5, 10.0, 99.9, 20.1, 20.1], # Outlier?
    "flag": [True, False, True, False, True, True], # Boolean
    "constant": ["X", "X", "X", "X", "X", "X"], # Zero variance
    "date": [date(2025, 1, 1), date(2025, 1, 2), date(2025, 1, 3), date(2025, 1, 4), date(2025, 1, 5), date(2025, 1, 5)] # Date column
    #pl.date_range(datetime.date(2025, 1, 1), datetime.date(2025, 1, 6), interval="1d", eager=True)
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
### Output
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

## Column Missing Proportion
### Setup
```python
missing_cols = column_missing_prop(df)
print(missing_cols)
```
### Output
```plaintext
shape: (6, 3)
┌──────────┬───────────┬──────────────┐
│ column   ┆ missing_n ┆ missing_prop │
│ ---      ┆ ---       ┆ ---          │
│ str      ┆ u32       ┆ f64          │
╞══════════╪═══════════╪══════════════╡
│ id       ┆ 0         ┆ 0.0          │
│ group    ┆ 1         ┆ 0.1667       │ # 1 missing value out of 6 rows
│ value    ┆ 0         ┆ 0.0          │
│ flag     ┆ 0         ┆ 0.0          │
│ constant ┆ 0         ┆ 0.0          │
│ date     ┆ 0         ┆ 0.0          │
└──────────┴───────────┴──────────────┘
```

## Row Missing Proportion
### Setup
```python
missing_rows = row_missing_prop(df)
print(missing_rows)
```
### Output
```plaintext

shape: (6, 3)
┌───────────┬───────────┬──────────────┐
│ row_index ┆ missing_n ┆ missing_prop │
│ ---       ┆ ---       ┆ ---          │
│ u32       ┆ u32       ┆ f64          │
╞═══════════╪═══════════╪══════════════╡
│ 1         ┆ 0         ┆ 0.0          │
│ 2         ┆ 0         ┆ 0.0          │
│ 3         ┆ 0         ┆ 0.0          │
│ 4         ┆ 1         ┆ 0.1667       │ # row 4 has 1 missing in group column out of 6 columns   
│ 5         ┆ 0         ┆ 0.0          │
│ 6         ┆ 0         ┆ 0.0          │
└───────────┴───────────┴──────────────┘
```
## Column Duplicate Indicator
### Setup
```python
dup_cols = column_dup_ind(df_issues)
print(dup_cols)
```
### Output
```plaintext
shape: (3, 2)
┌────────┬─────────┐
│ column ┆ dup_ind │
│ ---    ┆ ---     │
│ str    ┆ u8      │
╞════════╪═════════╡
│ col_a  ┆ 1       │ # Duplicate of col_b
│ col_b  ┆ 1       │ # Duplicate of col_a
│ col_c  ┆ 0       │
└────────┴─────────┘
```

## Row Duplicate Indicator
### Setup
```python
dup_rows = row_dup_ind(df)
print(dup_rows)
```
### Output
```plaintext
shape: (6, 2)
┌───────────┬─────────┐
│ row_index ┆ dup_ind │
│ ---       ┆ ---     │
│ u32       ┆ u8      │
╞═══════════╪═════════╡
│ 1         ┆ 0       │
│ 2         ┆ 0       │
│ 3         ┆ 0       │
│ 4         ┆ 0       │
│ 5         ┆ 1       │ # Duplicate of row 6 based on all row values
│ 6         ┆ 1       │ # Duplicate of row 5 based on all row values
└───────────┴─────────┘
```

## Numeric Statistics
### Setup
```python
# First, get column types
col_types = column_type_ident(df_issues)
# Calculate numeric stats (Note, getting col_types first is optional as num_stats will calculate it internally with specified thresholds)
num_summary = num_stats(df_issues, df_col_types=col_types)
print(num_summary.select([
    "column", "n", "mean", "std", "sparsity", "nan_ind", "inf_ind"
]))
```
### Output
```plaintext
shape: (3, 7)
┌────────┬─────┬──────┬──────────┬──────────┬─────────┬─────────┐
│ column ┆ n   ┆ mean ┆ std      ┆ sparsity ┆ nan_ind ┆ inf_ind │
│ ---    ┆ --- ┆ ---  ┆ ---      ┆ ---      ┆ ---     ┆ ---     │
│ str    ┆ u32 ┆ f64  ┆ f64      ┆ f64      ┆ u8      ┆ u8      │
╞════════╪═════╪══════╪══════════╪══════════╪═════════╪═════════╡
│ col_a  ┆ 6   ┆ 2.0  ┆ 0.894427 ┆ 0.0      ┆ 0       ┆ 0       │
│ col_b  ┆ 6   ┆ 2.0  ┆ 0.894427 ┆ 0.0      ┆ 0       ┆ 0       │
│ col_c  ┆ 2   ┆ 5.5  ┆ 0.707107 ┆ 0.0      ┆ 1       ┆ 1       │ # n=2 (ignored nan/inf/null), has raised NaN, has Inf
└────────┴─────┴──────┴──────────┴──────────┴─────────┴─────────┘
```

## Numeric Outlier Statistics
### Setup
```python
# Use the main sample df
col_types = column_type_ident(df)
col_outliers, row_outliers = num_outlier_stats(df, df_col_types=col_types, IQR_multi=1.5) # Common 1.5 multiplier

print("--- Column Outlier Stats ---")
print(col_outliers.filter(pl.col("column") == "value")) # Focus on 'value' col

print("\n--- Row Outlier Stats (Rows with outliers) ---")
print(row_outliers.filter(pl.col("outliers_ind") == 1))
```
### Output
```plaintext
--- Column Outlier Stats ---
┌────────┬────────────┬────────────┬──────────────┬────────────┬───────────────┐
│ column ┆ outlier_LB ┆ outlier_UB ┆ outliers_ind ┆ outliers_n ┆ outliers_prop │
│ ---    ┆ ---        ┆ ---        ┆ ---          ┆ ---        ┆ ---           │
│ str    ┆ f64        ┆ f64        ┆ u8           ┆ u32        ┆ f64           │
╞════════╪════════════╪════════════╪══════════════╪════════════╪═══════════════╡
│ value  ┆ -5.15      ┆ 35.25      ┆ 0            ┆ 1          ┆ 0.166667      │ # One outlier (99.9) in row 6
└────────┴────────────┴────────────┴──────────────┴────────────┴───────────────┘

--- Row Outlier Stats (Rows with outliers) ---
shape: (1, 4)
┌───────────┬──────────────┬────────────┬───────────────┐
│ row_index ┆ outliers_ind ┆ outliers_n ┆ outliers_prop │
│ ---       ┆ ---          ┆ ---        ┆ ---           │
│ u32       ┆ u8           ┆ u32        ┆ f64           │
╞═══════════╪══════════════╪════════════╪═══════════════╡
│ 4         ┆ 1            ┆ 1          ┆ 0.5           │
└───────────┴──────────────┴────────────┴───────────────┘
```
<!-- ## Categorical Statistics
### Setup
```python
# Use the main sample df
col_types = column_type_ident(df, unique_n_threshold=4, unique_prop_threshold=None) # Use thresholds for categorical identification

# Example 1: Include Null as a level, rare threshold = 1
col_cat_null, row_rare_null = cat_stats(
    df, df_col_types=col_types, exclude_null_level=False, rare_level_n_threshold=1
)
print("--- Cat Stats (Inc Null, Rare Freq <= 1) ---")
print(col_cat_null.filter(pl.col("column") == "group").select(pl.col([
    "column", "levels_n", "level_list", "level_freq_list", "rare_level_ind", "rare_level_n"
])))
print("\nRows containing rare levels (Inc Null):")
print(row_rare_null.filter(pl.col("rare_level_ind") == 1))

# Example 2: Exclude Null, rare threshold = 2
col_cat_excl, row_rare_excl = cat_stats(
    df, df_col_types=col_types, exclude_null_level=True, rare_level_n_threshold=2
)
print("\n--- Cat Stats (Excl Null, Rare Freq <= 2) ---")
print(col_cat_excl.filter(pl.col("column") == "group").select([
    "column", "levels_n", "level_list", "level_freq_list", "rare_level_ind", "rare_level_n"
]))
print("\nRows containing rare levels (Excl Null):")
print(row_rare_excl.filter(pl.col("rare_level_ind") == 1))
``` -->