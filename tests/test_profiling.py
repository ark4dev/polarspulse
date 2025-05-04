# tests/test_profiling.py
import pytest
import polars as pl
from polars.testing import assert_frame_equal, assert_series_equal
import math # For checking NaN

# Import functions from your package
from polarspulse import (
    profile,
    column_type_ident,
    column_missing_prop,
    row_missing_prop,
    column_dup_ind,
    row_dup_ind,
    num_stats,
    num_outlier_stats,
    cat_stats
)

# --- Fixtures ---
@pytest.fixture
def sample_df():
    """A diverse sample DataFrame for general testing."""
    return pl.DataFrame({
        "id": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 10], # Row 10 is duplicate of 9 based on id, value1
        "category": ["A", "B", "A", "C", "B", "A", "A", None, "C", "B", "B"], # Null, 'C' is rare?
        "value1": [10.1, 12.5, 9.8, 50.3, 11.0, 9.9, 10.5, 13.0, 1000.0, 11.5, 11.5], # Outlier, NaN, Inf tests needed separately
        "value2": [0, 1, 0, 1, 0, 0, 0, 1, 1, 0, 0], # Binary-like
        "all_same": [5, 5, 5, 5, 5, 5, 5, 5, 5, 5, 5], # Zero variance
        "bool_col": [True, False, True, False, True, True, False, True, False, True, True],
        "date_col": pl.date_range(pl.Date(2024, 1, 1), pl.Date(2024, 1, 11), interval="1d", eager=True)
    })

@pytest.fixture
def empty_df():
    """An empty DataFrame."""
    return pl.DataFrame()

@pytest.fixture
def df_with_issues():
     """DataFrame with NaN, Inf, more varied types."""
     return pl.DataFrame({
        "a": [1.0, None, 3.0, float('nan'), 5.0],
        "b": [None, None, None, None, None],
        "c": [1.0, 2.0, float('inf'), -float('inf'), 5.0],
        "d": ["x", "y", "x", "z", "y"],
        "e": [1, 1, 1, 1, 1] # zero var int
     })

# --- Test Cases ---

# Test main profile function
def test_profile_runs_structure(sample_df):
    """Test profile runs and returns expected structure."""
    data_summary, col_summary, row_summary = profile(sample_df)
    assert isinstance(data_summary, pl.DataFrame)
    assert isinstance(col_summary, pl.DataFrame)
    assert isinstance(row_summary, pl.DataFrame)
    assert data_summary.height == 1
    assert col_summary.height == sample_df.width
    assert row_summary.height == sample_df.height
    assert "column" in col_summary.columns
    assert "row_index" in row_summary.columns

def test_profile_empty_df(empty_df):
    """Test profile function with an empty DataFrame."""
    with pytest.raises(ValueError, match="The DataFrame is empty."):
        profile(empty_df)

def test_profile_toggles(sample_df):
    """Test that toggles in profile function work."""
     # Test with only missing stats
    _, col_summary, row_summary = profile(sample_df,
                                          get_dup_stats=False, get_num_stats=False,
                                          outlier_stats=False, get_cat_stats=False)
    assert "missing_n" in col_summary.columns
    assert "missing_n" in row_summary.columns
    assert "dup_ind" not in col_summary.columns
    assert "dup_ind" not in row_summary.columns
    assert "mean" not in col_summary.columns # Example num stat
    assert "outliers_n" not in col_summary.columns # Example outlier stat
    assert "levels_n" not in col_summary.columns # Example cat stat

# --- Test Individual Functions ---

# column_type_ident
def test_column_type_ident(sample_df):
    col_types = column_type_ident(sample_df, unique_n_threshold=5, unique_prop_threshold=0.2)
    assert col_types.filter(pl.col("column") == "id")["col_class"].item() == "num"
    assert col_types.filter(pl.col("column") == "category")["col_class"].item() == "cat" # 3 unique <= 5
    assert col_types.filter(pl.col("column") == "value1")["col_class"].item() == "num"
    assert col_types.filter(pl.col("column") == "value2")["col_class"].item() == "cat" # 2 unique <= 5
    assert col_types.filter(pl.col("column") == "all_same")["col_class"].item() == "zero_var"
    assert col_types.filter(pl.col("column") == "bool_col")["col_class"].item() == "cat" # 2 unique <= 5
    assert col_types.filter(pl.col("column") == "date_col")["col_class"].item() == "time"
    # Check thresholds used were calculated correctly (example)
    # df_n = 11, unique_n=5, unique_prop=0.2 -> prop_count = max(1, int(11*0.2)) = 2. Threshold used = min(5, 2) = 2
    assert col_types.filter(pl.col("column") == "category")["cat_n_threshold_used"].item() == 2


# missing stats
def test_column_missing_prop(sample_df, df_with_issues):
    miss_stats = column_missing_prop(sample_df)
    assert miss_stats.filter(pl.col("column") == "category")["missing_n"].item() == 1
    assert pytest.approx(miss_stats.filter(pl.col("column") == "category")["missing_prop"].item()) == 1/11

    miss_stats_issues = column_missing_prop(df_with_issues)
    assert miss_stats_issues.filter(pl.col("column") == "a")["missing_n"].item() == 1 # Null counts NaN as missing here
    assert miss_stats_issues.filter(pl.col("column") == "b")["missing_n"].item() == 5


def test_row_missing_prop(sample_df):
    row_miss = row_missing_prop(sample_df)
    assert row_miss.filter(pl.col("row_index") == 8)["missing_n"].item() == 1 # Row 8 has one null in category
    assert row_miss.filter(pl.col("row_index") != 8)["missing_n"].sum() == 0 # All other rows have 0 nulls


# duplicate stats
def test_column_dup_ind(sample_df):
     # Add a duplicate column for testing
     df = sample_df.with_columns(value1_dup = pl.col("value1"))
     dup_stats = column_dup_ind(df)
     # Note: transpose().is_duplicated() marks the *second* occurrence onwards as duplicate
     assert dup_stats.filter(pl.col("column") == "value1")["dup_ind"].item() == 0
     assert dup_stats.filter(pl.col("column") == "value1_dup")["dup_ind"].item() == 1

def test_row_dup_ind(sample_df):
    dup_stats = row_dup_ind(sample_df)
    assert dup_stats["dup_ind"].sum() == 1 # Only the 11th row is a duplicate of the 10th
    assert dup_stats.filter(pl.col("row_index") == 11)["dup_ind"].item() == 1
    assert dup_stats.filter(pl.col("row_index") == 10)["dup_ind"].item() == 0


# num_stats
def test_num_stats_basic(sample_df):
    col_types = column_type_ident(sample_df)
    num_summary = num_stats(sample_df, df_col_types=col_types)
    val1_stats = num_summary.filter(pl.col("column") == "value1")
    assert val1_stats["n"].item() == 11 # All are finite
    assert val1_stats["mean"].item() > 100 # Due to outlier 1000.0
    assert val1_stats["sparsity"].item() == 0 # No zeros
    assert val1_stats["nan_ind"].item() == 0
    assert val1_stats["inf_ind"].item() == 0

    val2_stats = num_summary.filter(pl.col("column") == "value2")
    assert val2_stats["n"].item() == 11
    assert pytest.approx(val2_stats["mean"].item()) == 4/11
    assert pytest.approx(val2_stats["sparsity"].item()) == 7/11 # 7 zeros

def test_num_stats_nan_inf(df_with_issues):
    col_types = column_type_ident(df_with_issues)
    num_summary = num_stats(df_with_issues, df_col_types=col_types)

    stats_a = num_summary.filter(pl.col("column") == "a")
    assert stats_a["n"].item() == 3 # 1, 3, 5 (null and nan ignored)
    assert stats_a["mean"].item() == 3.0
    assert stats_a["nan_ind"].item() == 1 # Contains NaN
    assert stats_a["inf_ind"].item() == 0

    stats_c = num_summary.filter(pl.col("column") == "c")
    assert stats_c["n"].item() == 3 # 1, 2, 5 (inf ignored)
    assert pytest.approx(stats_c["mean"].item()) == (1+2+5)/3
    assert stats_c["nan_ind"].item() == 0
    assert stats_c["inf_ind"].item() == 1 # Contains Inf


# num_outlier_stats
def test_num_outlier_stats(sample_df):
     col_types = column_type_ident(sample_df)
     col_out, row_out = num_outlier_stats(sample_df, df_col_types=col_types, IQR_multi=1.5) # Use common 1.5 for test

     val1_out = col_out.filter(pl.col("column") == "value1")
     assert val1_out["outliers_n"].item() >= 1 # 1000.0 should be an outlier
     assert val1_out["outliers_ind"].item() == 1

     val2_out = col_out.filter(pl.col("column") == "value2")
     assert val2_out["outliers_n"].item() == 0 # Binary 0/1 unlikely outliers with IQR method
     assert val2_out["outliers_ind"].item() == 0

     # Check row where value1=1000.0 (row index 9)
     assert row_out.filter(pl.col("row_index") == 9)["outliers_n"].item() >= 1
     assert row_out.filter(pl.col("row_index") == 9)["outliers_ind"].item() == 1


# cat_stats
def test_cat_stats_basic(sample_df):
    col_types = column_type_ident(sample_df)
    # Test with exclude_null_level=False first
    col_cat, row_rare = cat_stats(sample_df, df_col_types=col_types, exclude_null_level=False, rare_level_n_threshold=1)

    cat_col_stats = col_cat.filter(pl.col("column") == "category")
    assert cat_col_stats["levels_n"].item() == 4 # A, B, C, NULL
    assert "NULL" in cat_col_stats["level_list"].item()
    assert cat_col_stats["most_common_level"].item() == "A" # Freq A=4, B=4, C=2, NULL=1 -> depends on sort stability
    # Check rare level (threshold = 1, so NULL and C might be rare)
    assert cat_col_stats["rare_level_n"].item() >= 1 # NULL has freq 1
    assert cat_col_stats["rare_level_ind"].item() == 1
    assert "NULL" in cat_col_stats["rare_levels_list"].item()

    # Test row with NULL category (row index 8)
    assert row_rare.filter(pl.col("row_index") == 8)["rare_level_ind"].item() == 1 # Contains NULL which is rare

    # Test with exclude_null_level=True
    col_cat_ex, row_rare_ex = cat_stats(sample_df, df_col_types=col_types, exclude_null_level=True, rare_level_n_threshold=1)
    cat_col_stats_ex = col_cat_ex.filter(pl.col("column") == "category")
    assert cat_col_stats_ex["levels_n"].item() == 3 # A, B, C
    assert "NULL" not in cat_col_stats_ex["level_list"].item()
    assert cat_col_stats_ex["rare_level_n"].item() == 0 # C has freq 2 > 1
    assert cat_col_stats_ex["rare_level_ind"].item() == 0

    assert row_rare_ex.filter(pl.col("row_index") == 8)["rare_level_ind"].item() == 0 # NULL excluded


# TODO: Add many more specific tests for edge cases, parameter variations,
# different data types, and correctness checks against known outputs.