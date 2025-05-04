## PolarsPulse 🚀
*Fast, insightful data profiling for Polars DataFrames.*

PolarsPulse provides a suite of tools to quickly generate comprehensive data quality and descriptive statistics reports for your Polars DataFrames. It helps you understand your data's structure, identify potential issues like missing values, duplicates, outliers, and rare categories, and get a statistical overview, all leveraging the speed of Polars.

## Installation
```bash
pip install polarspulse
```

## Quick Start
```python
import polars as pl
from polarspulse import profile

# Create a sample DataFrame (replace with your actual data)
df = pl.DataFrame({
    "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10], # Duplicate ID
    "category": ["A", "B", "A", "C", "B", "A", "A", None, "C", "B", "B"], # Missing, Rare C?
    "value1": [10.1, 12.5, 9.8, 50.3, 11.0, 9.9, 10.5, 13.0, 1000.0, 11.5, 11.5], # Outlier? Duplicate row?
    "value2": [0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0], # Binary/Sparse
    "all_same": [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5], # Zero Variance
    "date": pl.date_range(pl.Date(2024, 1, 1), pl.Date(2024, 1, 11), interval="1d", eager=True)
})

# Generate the profile reports
# (Using default thresholds for classification, outliers, rare levels etc.)
data_summary, column_summary, row_summary = profile(df)

# Explore the results
print("--- Data Summary ---")
print(data_summary)

print("\n--- Column Summary (Selected Columns) ---")
print(column_summary.select([
    "column", "col_class", "missing_prop", "dup_ind", "mean", "std", "sparsity",
    "outliers_ind", "rare_level_ind"
]))

print("\n--- Row Summary (First 5 Rows) ---")
print(row_summary.head(5))
```

This will produce three Polars DataFrames:
1. data_summary: A single-row DataFrame with overall statistics about the dataset (dimensions, memory, counts/indicators of problematic columns/rows).
2. column_summary: A DataFrame where each row corresponds to a column in the original data, containing detailed statistics (type classification, missingness, duplicates, numerical stats, outlier info, categorical info).
3.  row_summary: A DataFrame where each row corresponds to a row in the original data, containing row-level statistics (missingness, duplicate status, outlier presence, rare level presence).

## Core Functions
PolarsPulse is built around several core functions, orchestrated by the main profile function:

column_type_ident: Classifies columns into 'numerical', 'categorical', 'time', 'zero_variance', or 'other' based on data type and the number/proportion of unique values.
column_missing_prop / row_missing_prop: Calculate the count and proportion of missing (Null) values per column or per row.
column_dup_ind / row_dup_ind: Identify duplicate columns or rows based on their values.
num_stats: Computes detailed descriptive statistics for numerical columns (mean, std, quantiles, skewness, kurtosis, sparsity, range, IQR, CV, NaN/Inf indicators).
num_outlier_stats: Detects outliers in numerical columns using a robust IQR method applied to scaled data (value - median / IQR) and provides outlier counts/indicators per column and per row.
cat_stats: Analyzes categorical columns, providing frequency counts/proportions for each level, Gini index, cardinality, and identifies rare levels based on frequency thresholds. Generates indicators for columns containing rare levels and rows containing rare level values.
profile: The main entry point that calls the relevant underlying functions based on user flags (e.g., get_miss_stats=True, outlier_stats=True) and aggregates the results into the three summary DataFrames (data_profile, col_profile, row_profile).

### Output
```plaintext
--- Data Summary ---
```

## Output Metrics Details
The profile function generates comprehensive metrics across three views: Overall Data, By Column, and By Row. The following table details the columns produced in the column_summary and row_summary DataFrames, as well as the fields in the data_summary DataFrame.

| Profile View | Function          | Column                      | Description                                                                       |
| ------------ | ----------------- | --------------------------- | --------------------------------------------------------------------------------- |
| By Column    | column_type_ident | approx_n_unique             | The approximate number of unique values in the column.                            |
| By Column    | column_type_ident | approx_prop_unique          | The approximate proportion of unique values in the column.                        |
| By Column    | column_type_ident | col_dtype                   | The data type of the column.                                                      |
| By Column    | column_type_ident | cat_n_threshold_used        | The absolute unique count threshold used for cat/num classification.              |
| By Column    | column_type_ident | cat_prop_threshold_used     | The unique proportion threshold used for cat/num classification.                  |
| By Column    | column_type_ident | col_class                   | The classified type ('cat', 'num', 'time', 'zero_var', 'other').                  |
| By Column    | miss_stats        | missing_n                   | Number of missing values (Nulls).                                                 |
| By Column    | miss_stats        | missing_prop                | Proportion of missing values (Nulls).                                             |
| By Column    | dup_stats         | dup_ind                     | An indicator (0/1) if the column is a duplicate of another column (by value).     |
| By Column    | num_stats         | n                           | The number of non-null, finite values.                                            |
| By Column    | num_stats         | sum                         | Column sum (excluding null/inf/nan).                                              |
| By Column    | num_stats         | mean                        | Column mean (excluding null/inf/nan).                                             |
| By Column    | num_stats         | std                         | Column standard deviation (excluding null/inf/nan).                               |
| By Column    | num_stats         | min                         | Minimum value (excluding null/inf/nan).                                           |
| By Column    | num_stats         | 1th                         | 1st percentile (excluding null/inf/nan).                                          |
| By Column    | num_stats         | 5th                         | 5th percentile (excluding null/inf/nan).                                          |
| By Column    | num_stats         | 10th                        | 10th percentile (excluding null/inf/nan).                                         |
| By Column    | num_stats         | 25th                        | 25th percentile (Q1) (excluding null/inf/nan).                                    |
| By Column    | num_stats         | 50th                        | 50th percentile (Median) (excluding null/inf/nan).                                |
| By Column    | num_stats         | 75th                        | 75th percentile (Q3) (excluding null/inf/nan).                                    |
| By Column    | num_stats         | 90th                        | 90th percentile (excluding null/inf/nan).                                         |
| By Column    | num_stats         | 95th                        | 95th percentile (excluding null/inf/nan).                                         |
| By Column    | num_stats         | 99th                        | 99th percentile (excluding null/inf/nan).                                         |
| By Column    | num_stats         | max                         | Maximum value (excluding null/inf/nan).                                           |
| By Column    | num_stats         | skew                        | Skewness (excluding null/inf/nan).                                                |
| By Column    | num_stats         | kurtosis                    | Kurtosis (excluding null/inf/nan).                                                |
| By Column    | num_stats         | sparsity                    | Proportion of zero values (excluding null/inf/nan).                               |
| By Column    | num_stats         | iqr                         | Interquartile Range (Q3 - Q1) (excluding null/inf/nan).                           |
| By Column    | num_stats         | range                       | Difference between max and min (excluding null/inf/nan).                          |
| By Column    | num_stats         | cv                          | Coefficient of Variation (std/mean) (excluding null/inf/nan).                     |
| By Column    | num_stats         | nan_ind                     | An indicator (0/1) if the column contains NaN values.                             |
| By Column    | num_stats         | inf_ind                     | An indicator (0/1) if the column contains Infinite values.                        |
| By Column    | num_stats         | high_skew_ind               | An indicator (0/1) if absolute skewness exceeds threshold.                        |
| By Column    | num_stats         | high_kurtosis_ind           | An indicator (0/1) if absolute kurtosis exceeds threshold.                        |
| By Column    | num_stats         | high_sparsity_ind           | An indicator (0/1) if sparsity (proportion of zeros) exceeds threshold.           |
| By Column    | num_stats         | high_cv_ind                 | An indicator (0/1) if absolute coefficient of variation exceeds threshold.        |
| By Column    | outlier_stats     | outlier_LB                  | Variable Lower Bound threshold for outlier detection (IQR method on scaled data). |
| By Column    | outlier_stats     | outlier_UB                  | Variable Upper Bound threshold for outlier detection (IQR method on scaled data). |
| By Column    | outlier_stats     | outliers_ind                | An indicator (0/1) if column includes outliers (based on LB/UB).                  |
| By Column    | outlier_stats     | outliers_n                  | The number of outliers identified in the column.                                  |
| By Column    | outlier_stats     | outliers_prop               | The proportion of outliers identified in the column.                              |
| By Column    | cat_stats         | level                       | The unique levels found in the categorical column (sorted by frequency).          |
| By Column    | cat_stats         | level_freq                  | The frequency count for each corresponding level.                                 |
| By Column    | cat_stats         | level_prop                  | The proportion for each corresponding level.                                      |
| By Column    | cat_stats         | gini_index                  | Gini impurity index (measure of level inequality).                                |
| By Column    | cat_stats         | levels_n                    | The number of unique levels (cardinality).                                        |
| By Column    | cat_stats         | most_common_level           | The most frequent level in the column.                                            |
| By Column    | cat_stats         | most_common_level_prop      | The proportion of the most frequent level.                                        |
| By Column    | cat_stats         | least_common_level          | The least frequent level in the column.                                           |
| By Column    | cat_stats         | least_common_level_prop     | The proportion of the least frequent level.                                       |
| By Column    | cat_stats         | include_null_level_ind      | An indicator (0/1) if Nulls were treated as a level in frequency stats.           |
| By Column    | cat_stats         | rare_level_ind              | An indicator (0/1) if the column contains any rare levels.                        |
| By Column    | cat_stats         | rare_level_n                | The number of levels identified as rare.                                          |
| By Column    | cat_stats         | rare_level                  | The list of levels identified as rare.                                            |
| By Column    | cat_stats         | rare_level_n_threshold_used | The frequency count threshold used to identify rare levels.                       |
| By Row       | miss_stats        | row_index                   | Row identifier (starting from 1).                                                 |
| By Row       | miss_stats        | missing_n                   | The number of missing values in the row across all columns.                       |
| By Row       | miss_stats        | missing_prop                | The proportion of missing values in the row.                                      |
| By Row       | dup_stats         | dup_ind                     | An indicator (0/1) if the row is a duplicate of another row (by value).           |
| By Row       | outlier_stats     | outliers_n                  | The number of numeric columns in this row identified as outliers.                 |
| By Row       | outlier_stats     | outliers_prop               | Proportion of numeric columns in this row identified as outliers.                 |
| By Row       | outlier_stats     | outliers_ind                | An indicator (0/1) if the row contains at least one outlier value.                |
| By Row       | cat_stats         | rare_level_ind              | An indicator (0/1) if the row contains a rare level in any cat column.            |
| Data Overall | profile           | number_of_rows              | Total number of rows in the DataFrame.                                            |
| Data Overall | profile           | number_of_cols              | Total number of columns in the DataFrame.                                         |
| Data Overall | profile           | memory_size_kb              | Estimated memory usage of the DataFrame in Kilobytes.                             |
| Data Overall | miss_stats        | col_max_miss_prop           | The maximum missing proportion found across all columns.                          |
| Data Overall | miss_stats        | row_max_miss_prop           | The maximum missing proportion found across all rows.                             |
| Data Overall | dup_stats         | col_dups_ind                | An indicator (0/1) if any duplicate columns exist (by value).                     |
| Data Overall | dup_stats         | row_dups_ind                | An indicator (0/1) if any duplicate rows exist (by value).                        |
| Data Overall | num_stats         | num_col_nan_ind             | An indicator (0/1) if any numeric column contains NaN values.                     |
| Data Overall | num_stats         | num_col_inf_ind             | An indicator (0/1) if any numeric column contains Infinite values.                |
| Data Overall | num_stats         | num_col_high_skew_ind       | An indicator (0/1) if any numeric column has high skewness.                       |
| Data Overall | num_stats         | num_col_high_kurtosis_ind   | An indicator (0/1) if any numeric column has high kurtosis.                       |
| Data Overall | num_stats         | num_col_high_cv_ind         | An indicator (0/1) if any numeric column has high coefficient of variation.       |
| Data Overall | num_stats         | num_col_high_sparsity_ind   | An indicator (0/1) if any numeric column has high sparsity (many zeros).          |
| Data Overall | outlier_stats     | num_col_outliers_ind        | An indicator (0/1) if any numeric column contains outliers.                       |
| Data Overall | outlier_stats     | row_outliers_n              | The total number of rows containing at least one outlier value.                   |
| Data Overall | cat_stats         | cat_col_rare_level_ind      | An indicator (0/1) if any categorical column contains rare levels.                |