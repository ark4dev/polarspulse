# polarspulse/profiling.py
import polars as pl
from typing import Tuple # Added for type hints

# --- Helper Functions (Keep all functions from the original code here) ---

# Function to compute column types and unique value counts
def column_type_ident(df: pl.DataFrame, unique_n_threshold:int = 10, unique_prop_threshold:float = 0.01) -> pl.DataFrame:
    """
    Classify columns in a DataFrame as categorical, numerical, time, zero_variance, or other
    based on unique value counts and data types.

    The effective unique value threshold used for classification is the minimum of
    `unique_n_threshold` and (`df.height` * `unique_prop_threshold`).

    :param df: A Polars DataFrame to classify columns.
    :param unique_n_threshold: The maximum number of unique values for a column to be classified as categorical.
    :param unique_prop_threshold: The proportion of unique values threshold for categorical classification (0 < threshold < 1).
    :return: A DataFrame with column names and their classifications, dtypes, and unique counts.
    :rtype: pl.DataFrame
    :raises ValueError: If thresholds are invalid or DataFrame is empty.
    """

    # Check if n_threshold is a positive integer
    if not isinstance(unique_n_threshold, int) or unique_n_threshold <= 0:
        raise ValueError("unique_n_threshold must be a positive integer.")

    # Check if prop_threshold is a float between 0 and 1
    # Allow None to disable prop_threshold
    if unique_prop_threshold is not None and (not isinstance(unique_prop_threshold, float) or not (0 < unique_prop_threshold < 1)):
        raise ValueError("unique_prop_threshold must be a float between 0 and 1, or None.")

    # Check if the DataFrame is not empty with at least one column and one row
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    # Get col stats
    col_types = [str(x) for x in df.dtypes]
    df_n = df.height

    # Get min unique values based on threshold (stricter of the two)
    prop_threshold_count = df_n # Default if unique_prop_threshold is None
    if unique_prop_threshold is not None:
        prop_threshold_count = int(df_n * unique_prop_threshold)

    # Ensure the threshold count is at least 1 if calculated from proportion
    prop_threshold_count = max(1, prop_threshold_count)

    cat_n_threshold_use = min(unique_n_threshold, prop_threshold_count)
    # Calculate the proportional threshold actually used (for reporting)
    cat_prop_threshold_use = cat_n_threshold_use / df_n

    # Compute column classifications
    col_unique_type = (
        df.select(pl.all())
        .fill_nan(None) # Treat NaN and Null as the same for uniqueness
        .unpivot(variable_name="column") # Transform data into long format
        .group_by("column", maintain_order=True) # Order needed
        .agg(pl.col("value").approx_n_unique().alias("approx_n_unique")) # Use approximate count for speed
        .with_columns(
            (pl.col("approx_n_unique") / pl.lit(df_n)).round(4).alias("approx_prop_unique"), # Increased precision
            pl.Series(name="col_dtype", values=col_types)
        )
        # Add thresholds used for classification reporting
        .with_columns(
            cat_n_threshold_used = pl.lit(cat_n_threshold_use).cast(pl.UInt32),
            cat_prop_threshold_used = pl.lit(cat_prop_threshold_use).cast(pl.Float64).round(4) # Increased precision
        )
        # Apply classification logic
        .with_columns(
            col_class =
                # zero-variance vars: approx_n_unique <= 1
                pl.when(pl.col("approx_n_unique") <= 1).then(pl.lit("zero_var"))
                # time vars: check dtype first
                .when(pl.col("col_dtype").str.contains("Date|Duration|Time|Datetime")).then(pl.lit("time"))
                # cat vars: approx_n_unique <= cat_n_threshold_use and suitable dtype
                .when((pl.col("approx_n_unique") > 1) &
                      (pl.col("approx_n_unique") <= pl.lit(cat_n_threshold_use)) &
                      (pl.col("col_dtype").str.contains("Utf8|String|Binary|Boolean|Categorical|Enum|Int|UInt|Float")) # Broaden types slightly, Categorical/Enum
                     ).then(pl.lit("cat"))
                # num vars: approx_n_unique > cat_n_threshold_use and numeric dtype
                .when((pl.col("approx_n_unique") > 1) &
                      (pl.col("approx_n_unique") > pl.lit(cat_n_threshold_use)) &
                      (pl.col("col_dtype").str.contains("Float|Int|UInt"))
                     ).then(pl.lit("num"))
                .otherwise(pl.lit("other")) # Catch-all for remaining types/conditions
        )
    )

    return col_unique_type

# Function to compute missing data proportions
def column_missing_prop(df: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the count and proportion of missing values (Nulls) for each column.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    # Compute missing data counts and proportions
    na_counts = df.null_count().transpose(include_header=False, column_names=["missing_n"])["missing_n"]
    na_prop = (na_counts / df.height).round(4) # Increased precision

    return pl.DataFrame({
        "column": df.columns,
        "missing_n": na_counts.cast(pl.UInt32),
        "missing_prop": na_prop
    })

# Function to compute row-wise missing data proportions
def row_missing_prop(df: pl.DataFrame) -> pl.DataFrame:
    """
    Computes the count and proportion of missing values (Nulls) for each row.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    return (
        df.select( # Avoid modifying original df implicitly
            pl.sum_horizontal(pl.all().is_null()).alias("missing_n")
        )
        .with_columns(
            (pl.col("missing_n") / pl.lit(df.width)).round(4).alias("missing_prop") # Increased precision
        )
        .with_row_index("row_index", offset=1) # Add row_index (UInt32 default)
        .select(["row_index", "missing_n", "missing_prop"]) # Select and order columns
       )

# Function to compute indicator for duplicate columns
def column_dup_ind(df: pl.DataFrame)-> pl.DataFrame:
    """
    Identifies duplicate columns based on their values (not names).
    Returns an indicator (0/1) for each column.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    # Transposing can be memory intensive for wide dataframes
    if df.width > 1000: # Add a heuristic warning or alternative approach?
         print("Warning: Detecting duplicate columns on wide DataFrames (>{df.width} cols) can be slow/memory intensive.")

    return pl.DataFrame({
        "column": df.columns,
        "dup_ind": df.transpose().is_duplicated().cast(pl.UInt8) # Use UInt8 for indicator
    })

# Function to compute indicator for duplicate rows
def row_dup_ind(df: pl.DataFrame)-> pl.DataFrame:
    """
    Identifies duplicate rows based on their values.
    Returns an indicator (0/1) for each row.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    return pl.DataFrame({
        "row_index": df.with_row_index(name="row_index", offset=1).select(pl.col("row_index")),
        "dup_ind": df.is_duplicated().cast(pl.UInt8) # Use UInt8 for indicator
    })

# Function to compute numeric column stats
def num_stats(df:pl.DataFrame,
              df_col_types:pl.DataFrame = None,
              unique_n_threshold: int = 10,
              unique_prop_threshold: float = 0.01,
              skew_threshold: float = 3.0,
              kurtosis_threshold: float = 3.0,
              sparsity_threshold: float = 0.5,
              cv_threshold: float = 1.0,
              ) -> pl.DataFrame:
    """
    Computes descriptive statistics for numeric columns.
    Includes mean, std, quantiles, skewness, kurtosis, sparsity, etc.
    All stats ignore Null, NaN, and Infinite values unless specified (e.g., nan/inf indicators).
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    # Check if df_col_types is provided, if not, compute it
    if df_col_types is None:
        df_col_types = column_type_ident(df, unique_n_threshold=unique_n_threshold, unique_prop_threshold=unique_prop_threshold)

    # Identify numeric columns
    num_cols = df_col_types.filter(pl.col("col_class") == "num").get_column("column").to_list()

    # Prepare empty result for non-numeric columns
    non_num_cols = df_col_types.filter(pl.col("col_class") != "num").get_column("column").to_list()
    non_num_col_stats = pl.DataFrame({"column": non_num_cols}).with_columns(pl.col("column").cast(pl.String)) # Ensure correct type for non-numeric columns

    if len(num_cols)==0: # No numeric columns found
        return non_num_col_stats # Return empty stats for non-numeric cols

    # Compute stats for numeric columns
    # Separate computation for NaN/Inf indicators as they need original data
    nan_inf_stats = (
         df.select(pl.col(num_cols))
         .unpivot(variable_name="column")
         .group_by("column", maintain_order=True)
         .agg(
             (pl.col("value").is_nan()).any().cast(pl.UInt8).alias("nan_ind"),
             (pl.col("value").is_infinite()).any().cast(pl.UInt8).alias("inf_ind")
         )
    )

    # Compute main descriptive stats excluding non-finite values
    main_stats = (
        df.select(pl.col(num_cols))
        .unpivot(variable_name="column")
        .filter(pl.col("value").is_finite()) # filter out NaN and Infinite values
        .group_by("column", maintain_order=True)
        .agg(
            pl.col("value").len().cast(pl.UInt32).alias("n"),
            pl.col("value").sum().alias("sum"),
            pl.col("value").mean().alias("mean"),
            pl.col("value").std().alias("std"),
            pl.col("value").min().alias("min"),
            pl.col("value").quantile(0.01).alias("1th"),
            pl.col("value").quantile(0.05).alias("5th"),
            pl.col("value").quantile(0.10).alias("10th"),
            pl.col("value").quantile(0.25).alias("25th"),
            pl.col("value").quantile(0.50).alias("50th"),
            pl.col("value").quantile(0.75).alias("75th"),
            pl.col("value").quantile(0.90).alias("90th"),
            pl.col("value").quantile(0.95).alias("95th"),
            pl.col("value").quantile(0.99).alias("99th"),
            pl.col("value").max().alias("max"),
            pl.col("value").skew().alias("skew"), # Can be null if std is 0
            pl.col("value").kurtosis().alias("kurtosis"), # Can be null if std is 0
            (pl.col("value") == 0).mean().alias("sparsity"), # Prop zeros among finite
        )
        .with_columns(
            # Derived stats - handle potential division by zero or nulls
            iqr = (pl.col("75th") - pl.col("25th")),
            range = (pl.col("max") - pl.col("min")),
            cv = pl.when(pl.col("mean") != 0).then(pl.col("std") / pl.col("mean")).otherwise(None) # Coef of variation
        )
         # Compute threshold indicators
        .with_columns(
             # Use fill_null(0) for skew/kurtosis if they are null (e.g., constant value)
            high_skew_ind = (pl.col("skew").fill_null(0).abs() > skew_threshold).cast(pl.UInt8),
            high_kurtosis_ind = (pl.col("kurtosis").fill_null(0).abs() > kurtosis_threshold).cast(pl.UInt8),
            high_sparsity_ind = (pl.col("sparsity") > sparsity_threshold).cast(pl.UInt8),
            # CV can be tricky (large for mean near zero), abs value helps
            high_cv_ind = pl.when(pl.col("cv").is_not_null())
                          .then(pl.col("cv").abs() > cv_threshold)
                          .otherwise(False) # Treat null CV as not high
                          .cast(pl.UInt8)
        )
    )

    # Combine stats and fill appropriately
    col_stats = (
        main_stats
        .join(nan_inf_stats, on="column", how="left") # Join NaN/Inf indicators
        .join(non_num_col_stats, on="column", how="full", coalesce=True) # Add back non-numeric columns
        .sort("column") # Maintain consistent column order
    )

    return col_stats

# Function to compute numeric outlier stats
def num_outlier_stats(df:pl.DataFrame,
                      df_col_types:pl.DataFrame = None,
                      unique_n_threshold: int = 10,
                      unique_prop_threshold: float = 0.01,
                      IQR_multi:float = 5.0 
                     ) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Identifies outliers in numeric columns using a robust IQR method on scaled data.
    Scaling: `(value - median) / IQR`
    Outlier if scaled value is outside `Q1_scaled - IQR_multi * IQR_scaled` or `Q3_scaled + IQR_multi * IQR_scaled`.
    Returns column-level and row-level outlier statistics.
    NaNs and Infinite values are ignored in outlier detection.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    if not isinstance(IQR_multi, (int, float)) or IQR_multi <= 0:
         raise ValueError("IQR_multi must be a positive number.")

    # Check if df_col_types is provided, if not, compute it
    if df_col_types is None:
        df_col_types = column_type_ident(df, unique_n_threshold=unique_n_threshold, unique_prop_threshold=unique_prop_threshold)

    # Identify numeric columns
    num_cols = df_col_types.filter(pl.col("col_class") == "num").get_column("column").to_list()

    # Prepare empty results for non-numeric columns / empty input
    non_num_cols = df_col_types.filter(pl.col("col_class") != "num").get_column("column").to_list()
    non_num_col_set = pl.DataFrame({"column": non_num_cols}).with_columns(pl.col("column").cast(pl.String)) # Ensure correct type for non-numeric columns
    empty_row_stats = pl.DataFrame(schema={"row_index": pl.UInt32}) # Ensure correct schema for empty case

    if len(num_cols)==0: # If no numeric columns found, return empty stats and print warning
        print("Warning: No numeric columns found in the DataFrame.")
        return non_num_col_set, empty_row_stats

    # Compute df_long for finite numeric values with row index
    df_long = (
        df.select(pl.col(num_cols))
        .with_row_index("row_index", offset=1)
        .unpivot(index="row_index", variable_name="column")
        .filter(pl.col("value").is_finite()) # Drop NaNs and infinite values
    )

    # Compute basic IQR stats on original data
    col_iqr_stats = (
        df_long
        .group_by("column", maintain_order=True)
        .agg(
            pl.col("value").quantile(0.25).alias("25th"),
            pl.col("value").quantile(0.5).alias("50th"), # Median
            pl.col("value").quantile(0.75).alias("75th")
        )
        .with_columns(
            iqr = (pl.col("75th") - pl.col("25th"))
        )
        # Handle cases where IQR is zero (constant values within Q1-Q3)
        .with_columns(
            iqr = pl.when(pl.col("iqr") == 0).then(1e-9).otherwise(pl.col("iqr")) # Replace 0 IQR with small epsilon to avoid division by zero
        )
    )

    # Compute scaled values and their IQR stats to find thresholds
    # Need to join back iqr_stats first
    df_long_scaled = df_long.join(col_iqr_stats, on="column", how="left")

    scaled_iqr_stats = (
         df_long_scaled
         # Compute scaled value: (value - median) / iqr
         .with_columns(scaled_value = (pl.col("value") - pl.col("50th")) / pl.col("iqr"))
         .group_by("column", maintain_order=True)
         .agg(
             pl.col("scaled_value").quantile(0.25).alias("scaled_value_25th"),
             # pl.col("scaled_value").quantile(0.5).alias("scaled_value_50th"), # Not needed for bounds
             pl.col("scaled_value").quantile(0.75).alias("scaled_value_75th")
         )
         .with_columns(
             scaled_value_iqr = pl.col("scaled_value_75th") - pl.col("scaled_value_25th")
         )
         # Calculate scaled bounds
         .with_columns(
             scaled_value_LB = pl.col("scaled_value_25th") - pl.lit(IQR_multi) * pl.col("scaled_value_iqr"),
             scaled_value_UB = pl.col("scaled_value_75th") + pl.lit(IQR_multi) * pl.col("scaled_value_iqr")
         )
    )

    # Compute final thresholds by transforming scaled bounds back to original scale
    outlier_thresholds = (
        scaled_iqr_stats
        .join(col_iqr_stats.select(["column", "50th", "iqr"]), on="column", how="left") # Join back median and IQR
        # Inverse scale transformation: scaled_bound * iqr + median
        .with_columns(
            outlier_LB = pl.col("scaled_value_LB") * pl.col("iqr") + pl.col("50th"),
            outlier_UB = pl.col("scaled_value_UB") * pl.col("iqr") + pl.col("50th")
        )
        .select(["column", "outlier_LB", "outlier_UB"])
    )

    # Compute indicators if values are outside the calculated bounds
    df_outlier_ind = (
        df_long # Use original long df with finite values
        .join(outlier_thresholds, on="column", how="left")
        .with_columns(
            # .not_() handles cases where value is exactly on the boundary correctly
            outliers_ind = pl.col("value").is_between(pl.col("outlier_LB"), pl.col("outlier_UB"), closed='both').not_().cast(pl.UInt8)
        )
        .select(["row_index", "column", "outliers_ind"]) # Keep only needed columns
    )

    # Outlier stats by col 
    col_outlier_ind = (
        # compute agg by column
        df_outlier_ind
        .group_by("column")
        .agg(outliers_n=(pl.col("outliers_ind")==pl.lit(1)).sum()) 
        .with_columns(
            outliers_prop=pl.col("outliers_n")/pl.lit(df.height),
            outliers_ind=(pl.col("outliers_n")>1).cast(pl.UInt8)
        )
        # add threshold info and order columns
        .join(outlier_thresholds, on="column")
        .select(["column", "outlier_LB", "outlier_UB", "outliers_ind", "outliers_n", "outliers_prop"])

        # add empty set for non-num columns
        .join(non_num_col_set, on="column", how="full", coalesce=True)
    )


    # Outlier stats by row
    row_outlier_ind = (
            # compute agg by column
            df_outlier_ind
            .group_by("row_index")
            .agg(outliers_n=(pl.col("outliers_ind")==pl.lit(1)).sum())
            .with_columns(
                outliers_prop=pl.col("outliers_n")/pl.lit(len(num_cols)), # Prop in reference to number or num columns per sample
                outliers_ind=(pl.col("outliers_n")>0).cast(pl.UInt8)
            )
            # order columns
            # .join(outlier_thresholds, on="column")
            .select(["row_index", "outliers_ind", "outliers_n", "outliers_prop"])
    )

    # # Combine with non-numeric columns
    # col_outlier_final = (
    #     col_outlier_agg
    #     .join(non_num_col_set, on="column", how="full", coalesce=True)
    #     .sort("column")
    # )


    # # Aggregate outlier stats by row
    # row_outlier_agg = (
    #     df_outlier_ind
    #     .group_by("row_index", maintain_order=True)
    #     .agg(
    #         outliers_n = pl.col("outliers_ind").sum().cast(pl.UInt32) # Count outliers per row
    #     )
    #     .with_columns(
    #          # Prop relative to number of numeric columns checked for that row
    #         outliers_prop = (pl.col("outliers_n") / pl.lit(len(num_cols))).round(4),
    #         outliers_ind = (pl.col("outliers_n") > 0).cast(pl.UInt8)
    #     )
    #     .select(["row_index", "outliers_ind", "outliers_n", "outliers_prop"])
    # )

    # # Ensure row output covers all original rows, filling with 0 if no outliers
    # all_rows_index = pl.DataFrame({"row_index": pl.int_range(1, df.height + 1, dtype=pl.UInt32)})
    # row_outlier_final = (
    #     all_rows_index
    #     .join(row_outlier_agg, on="row_index", how="left")
    #     .fill_null(0) # Rows with no outliers will have nulls after left join
    #     # Ensure correct dtypes after fill_null
    #     .with_columns(
    #          pl.col("outliers_ind").cast(pl.UInt8),
    #          pl.col("outliers_n").cast(pl.UInt32),
    #          pl.col("outliers_prop").cast(pl.Float64)
    #     )
    #     .sort("row_index")
    # )

    return col_outlier_ind, row_outlier_ind

# Function to identify and analyze categorical levels
def cat_stats(df: pl.DataFrame,
              df_col_types:pl.DataFrame = None,
              unique_n_threshold: int = 10,
              unique_prop_threshold: float = 0.01,
              exclude_null_level: bool = True,
              rare_level_n_threshold: int = 5,
              rare_level_prop_threshold: float = 0.01
             ) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Analyzes levels in categorical columns: frequency, Gini index, rare levels.
    If `exclude_null_level` is True, Nulls are ignored; otherwise, they are treated as a level "NULL".
    Rare levels are identified based on the minimum threshold derived from
    `rare_level_n_threshold` and `rare_level_prop_threshold`.
    Returns column-level frequency stats and row-level rare level indicators.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    # Input validation for rare level thresholds
    if not isinstance(rare_level_n_threshold, int) or rare_level_n_threshold < 0:
        raise ValueError("rare_level_n_threshold must be a non-negative integer.")
    if rare_level_prop_threshold is not None and (not isinstance(rare_level_prop_threshold, float) or not (0 <= rare_level_prop_threshold < 1)):
         raise ValueError("rare_level_prop_threshold must be a float between 0 and 1 (exclusive of 1), or None.")

    # Check if df_col_types is provided, if not, compute it
    if df_col_types is None:
        df_col_types = column_type_ident(df, unique_n_threshold=unique_n_threshold, unique_prop_threshold=unique_prop_threshold)

    # Identify categorical columns
    cat_cols = df_col_types.filter(pl.col("col_class") == "cat").get_column("column").to_list()

    # Prepare empty results for non-categorical columns / empty input
    non_cat_cols = df_col_types.filter(pl.col("col_class") != "cat").get_column("column").to_list()
    non_cat_col_set = pl.DataFrame({"column": non_cat_cols}).with_columns(pl.col("column").cast(pl.String))
    empty_row_stats = pl.DataFrame(schema={"row_index": pl.UInt32})

    if len(cat_cols)==0: # No categorical columns found, return empty stats and print warning
        print("Warning: No categorical columns found in the DataFrame.")
        return non_cat_col_set, empty_row_stats

    # Transform data to long-format, casting to String for consistent level handling
    df_cat_long = (
        df.select(cat_cols)
        .with_columns(pl.all().cast(pl.String)) # cast all string to ensure numeric columns are treated as catagorical variables
        .with_row_index("row_index", offset=1)
        .unpivot(index="row_index", variable_name="column", value_name="level")
    )

    # Apply condition for whether to drop null levels
    if exclude_null_level:
        df_cat_long = df_cat_long.drop_nulls(subset=["level"])
    else:
        # Add a null as a level to the long format data
        df_cat_long = df_cat_long.fill_null(value="NULL")

    # Calculate frequency counts for each column levels
    df_freq_counts = (
        df_cat_long
        .group_by(pl.all().exclude("row_index"))
        .agg(pl.len().alias("level_freq")) 
        .with_columns(level_prop=pl.col("level_freq")/pl.lit(df.height))
    )

    # Compute entropy statistics for each categorical column
    df_freq_disparity = (
        df_freq_counts
        .sort(["column",  "level_freq"], descending=True) # Sorting is applied to get min and max by level_freq
        .group_by("column", maintain_order=True)
        .agg(
            # entropy_bits = -1 * (pl.col("level_prop") * pl.col("level_prop").log(base=2)).sum(),
            gini_index = pl.lit(1) - (pl.col("level_prop")**2).sum(),
            levels_n = pl.col("level").n_unique().cast(pl.UInt32),
            most_common_level=pl.col("level").first(),                 # Get most frequent level
            most_common_level_prop=pl.col("level_prop").first(),
            least_common_level=pl.col("level").last(),                 # Get most frequent level
            least_common_level_prop=pl.col("level_prop").last(),
            )
        # Add indicator for null level included in the frequency counts
        .with_columns(include_null_level_ind = pl.lit(exclude_null_level).not_().cast(pl.UInt32)) # add indicator for null level
    )

    # --- Rare Level Detection ---
    # Set threshold to total number of data rows (i.e. no no rare levels detected all levels)
    if(rare_level_n_threshold is None and rare_level_prop_threshold is None):
        rare_level_n_threshold_use = None
        df_cat_rare_levels = pl.DataFrame(schema={"column":pl.String}) #, "rare_levels":pl.String, "rare_level_ind":int})
    else:
        n_threshold = rare_level_n_threshold if rare_level_n_threshold is not None else df_cat_long.height
        prop_threshold= int(df_cat_long.height * rare_level_prop_threshold) if rare_level_prop_threshold is not None else df_cat_long.height
        rare_level_n_threshold_use = min(n_threshold, prop_threshold) # Use the stricter (lower) threshold between count and proportion

        # Identify rare levels applying a filter
        df_cat_rare_levels = (
            df_freq_counts
            .with_columns(
                rare_level_ind=pl.when(pl.col("level_freq")<= rare_level_n_threshold_use)
                    .then(1).otherwise(0) 
            )
            .filter(rare_level_ind=1)
            .group_by("column")
            .agg(
                rare_level_ind=(pl.col("rare_level_ind")==pl.lit(1)).sum(),
                rare_level_n = pl.col("level").len(),
                rare_level=pl.col("level"),
                )
            .with_columns(rare_level_n_threshold_used=pl.lit(rare_level_n_threshold_use).cast(pl.UInt32)) # cast to Int32 for consistency
        )

    # column-level categorical stats
    col_cat_freq = (
            df_freq_counts
            .sort("level_freq", descending=True)
            .group_by("column", maintain_order=True)
            .agg(
                pl.col("level"),
                pl.col("level_freq"),
                pl.col("level_prop").round(4).alias("level_prop"),
                )
            .join(df_freq_disparity, on="column", how="full", coalesce=True)
            .join(df_cat_rare_levels, on="column", how="full", coalesce=True)
            .join(non_cat_col_set, on="column", how="full", coalesce=True)
        )
    
    # row-level rare level
    row_rare_level_ind = (
        df_cat_long
        .join((
            df_cat_rare_levels
            .explode("rare_level")
            .rename({"rare_level":"level"})
        ),  on=["column", "level"], how="full", coalesce=True)
        .group_by("row_index")
        .agg(rare_level_ind=(pl.col("rare_level_n").is_not_null().sum()>0).cast(pl.UInt8).fill_null(0))
        )

    return col_cat_freq, row_rare_level_ind

# --- Main Profile Function ---
def profile(df:pl.DataFrame,

            # Col Classification thresholds
            unique_n_threshold:int = 10,
            unique_prop_threshold:float = 0.01,

            # Toggles for sections
            get_miss_stats:bool = True,
            get_dup_stats:bool = True,
            get_num_stats:bool = True,
            outlier_stats:bool = True,
            get_cat_stats:bool = True,

            # Num stats thresholds
            skew_threshold: float = 3.0,
            kurtosis_threshold: float = 3.0,
            sparsity_threshold: float = 0.5,
            cv_threshold: float = 1.0,

            # Outlier stats threshold multiplier
            IQR_multi:float = 5.0,

            # Cat stats thresholds/options
            exclude_null_level: bool = True,
            rare_level_n_threshold: int = 5,
            rare_level_prop_threshold: float = 0.01

            ) -> Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    """
    Generates a comprehensive data profile for a Polars DataFrame.

    Computes statistics for columns and rows including:
    - Column type classification (numeric, categorical, time, etc.)
    - Missing data proportions (column-wise and row-wise)
    - Duplicate indicators (column-wise and row-wise, based on values)
    - Numeric column statistics (mean, std, quantiles, skew, kurtosis, etc.)
    - Outlier detection for numeric columns (IQR method on scaled data)
    - Categorical column analysis (level frequencies, Gini, rare levels)

    :param df: Input Polars DataFrame.
    :param unique_n_threshold: Max unique values for 'categorical' classification.
    :param unique_prop_threshold: Proportion unique values threshold for 'categorical'.
    :param get_miss_stats: Whether to compute missing value statistics.
    :param get_dup_stats: Whether to compute duplicate statistics.
    :param get_num_stats: Whether to compute numeric descriptive statistics.
    :param outlier_stats: Whether to compute numeric outlier statistics.
    :param get_cat_stats: Whether to compute categorical statistics.
    :param skew_threshold: Absolute threshold to flag high skewness.
    :param kurtosis_threshold: Absolute threshold to flag high kurtosis.
    :param sparsity_threshold: Threshold (proportion of zeros) to flag high sparsity.
    :param cv_threshold: Absolute threshold to flag high coefficient of variation.
    :param IQR_multi: Multiplier for IQR range in outlier detection.
    :param exclude_null_level: If True, Nulls are ignored in categorical analysis.
    :param rare_level_n_threshold: Absolute count threshold for rare category levels.
    :param rare_level_prop_threshold: Proportion threshold for rare category levels.

    :return: A tuple containing three DataFrames:
        1. data_profile: Overall summary statistics for the dataset.
        2. col_profile: Detailed statistics for each column.
        3. row_profile: Statistics for each row.
    :rtype: Tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]
    :raises ValueError: If the DataFrame is empty or thresholds are invalid.
    """
    if df.is_empty() or df.height == 0 or df.width == 0:
        raise ValueError("The DataFrame is empty.")

    # --- 1. Initial Column Classification ---
    df_col_types = column_type_ident(
        df=df,
        unique_n_threshold=unique_n_threshold,
        unique_prop_threshold=unique_prop_threshold
    )
    num_cols = df_col_types.filter(pl.col("col_class") == "num").get_column("column").to_list()
    cat_cols = df_col_types.filter(pl.col("col_class") == "cat").get_column("column").to_list()

    # --- Initialize Profile Components ---
    # Base column profile starts with type identification
    col_profile_list = [df_col_types]
    row_profile_list = [] # Start empty, add row index later if needed

    # Schemas for empty results if sections are skipped
    col_empty_df = pl.DataFrame(schema={"column":pl.String})
    row_empty_df = pl.DataFrame(schema={"row_index":pl.UInt32}) # Ensure correct schema for empty case
    # col_empty_df = pl.DataFrame({"column": df.columns}) # Use actual columns for joins
    # row_empty_df = pl.DataFrame({"row_index":pl.UInt32})

    # --- 2. Compute Optional Statistics ---
    # Missing Stats
    col_miss = col_empty_df
    row_miss = row_empty_df
    if get_miss_stats:
        col_miss = column_missing_prop(df)
        row_miss = row_missing_prop(df)
        col_profile_list.append(col_miss)
        row_profile_list.append(row_miss)

    # Duplicate Stats
    col_dup = col_empty_df
    row_dup = row_empty_df
    if get_dup_stats:
        col_dup = column_dup_ind(df)
        row_dup = row_dup_ind(df) # Assumes row_index is generated internally or joined
        col_profile_list.append(col_dup)
        row_profile_list.append(row_dup)


    # Numeric Stats
    col_num = col_empty_df
    if get_num_stats and len(num_cols)>0:
        col_num = num_stats(
            df=df, df_col_types=df_col_types,
            skew_threshold=skew_threshold, kurtosis_threshold=kurtosis_threshold,
            sparsity_threshold=sparsity_threshold, cv_threshold=cv_threshold
        )
        # Select only stat columns to avoid joining 'column' twice
        col_profile_list.append(col_num)

    # Outlier Stats
    col_outlier = col_empty_df
    row_outlier = row_empty_df
    if outlier_stats and len(num_cols)>0:
        col_outlier, row_outlier = num_outlier_stats(
            df=df, df_col_types=df_col_types, IQR_multi=IQR_multi
        )
        col_profile_list.append(col_outlier)
        row_profile_list.append(row_outlier)

    # Categorical Stats
    col_cat = col_empty_df
    row_rare = row_empty_df
    if get_cat_stats and len(cat_cols):
        col_cat, row_rare = cat_stats(
            df=df, df_col_types=df_col_types,
            exclude_null_level=exclude_null_level,
            rare_level_n_threshold=rare_level_n_threshold,
            rare_level_prop_threshold=rare_level_prop_threshold
        )
        col_profile_list.append(col_cat)
        row_profile_list.append(row_rare)

    # --- 3. Assemble Column and Row Profiles ---
    # Combine column stats - join progressively on 'column'
    col_profile = pl.concat(col_profile_list, how="align")

    # Combine row stats - join progressively on 'row_index'
    row_profile = pl.concat(row_profile_list, how="align")

    # --- 4. Generate Data Overall Summary ---
    data_profile = pl.DataFrame({
        "number_of_rows":df.height, #f"{df.height} x {df.width}",
        "number_of_cols": df.width,
        "memory_size_kb": df.estimated_size("kb")})
    if get_miss_stats:
        data_profile = data_profile.with_columns(
            col_max_miss_prop=col_profile["missing_prop"].max(),
            row_max_miss_prop=row_profile["missing_prop"].max(), 
        )
    if get_dup_stats:
        data_profile = data_profile.with_columns(
            # Summing indicators (0/1) gives count; > 0 means at least one duplicate
            col_dups_ind=pl.lit(col_profile["dup_ind"].sum()>0).cast(pl.UInt32), # Number of columns with one other matching column duplicate (always even) 
            row_dups_ind=pl.lit(row_profile["dup_ind"].sum()>0).cast(pl.UInt32), # Number of rows with one other matching row duplicate (always even)
         )
    if get_num_stats and len(num_cols)>0:
        data_profile = data_profile.with_columns(
            num_col_nan_ind=pl.lit(col_profile["nan_ind"].max()),
            num_col_inf_ind=pl.lit(col_profile["inf_ind"].max()),
            num_col_high_skew_ind=pl.lit(col_profile["high_skew_ind"].max()),
            num_col_high_kurtosis_ind=pl.lit(col_profile["high_kurtosis_ind"].max()),
            num_col_high_cv_ind=pl.lit(col_profile["high_cv_ind"].max()),
            num_col_high_sparsity_ind=pl.lit(col_profile["high_sparsity_ind"].max())
        )
    if outlier_stats and len(num_cols)>0:
             data_profile = data_profile.with_columns(
                num_col_outliers_n=pl.lit(col_profile["outliers_ind"].max()),
                row_outliers_n=pl.lit(row_profile["outliers_ind"].sum()),
            )
    if get_cat_stats and len(cat_cols)>0:
        data_profile = data_profile.with_columns(
            cat_col_rare_level_ind=pl.lit(col_profile["rare_level_ind"].sum()>0).cast(pl.UInt32), 
        )

    ## Cast indicators in col, row, and data profile to UInt8 for consistency
    # data profile
    indicator_cols = [c for c in data_profile.columns if c.endswith('_ind')]
    if len(indicator_cols):
        data_profile = data_profile.with_columns([
            pl.col(c).cast(pl.UInt8) for c in indicator_cols
        ])
    
    # col profile
    indicator_cols = [c for c in col_profile.columns if c.endswith('_ind')]
    if len(indicator_cols)>0:
        col_profile = col_profile.with_columns([
            pl.col(c).cast(pl.UInt8) for c in indicator_cols
        ])

    # row profile
    indicator_cols = [c for c in row_profile.columns if c.endswith('_ind')]
    if len(indicator_cols)>0:
        row_profile = row_profile.with_columns([
            pl.col(c).cast(pl.UInt8) for c in indicator_cols
        ])

    return data_profile, col_profile, row_profile