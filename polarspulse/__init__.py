# polarspulse/__init__.py
"""
PolarsPulse: Fast, insightful data profiling for Polars DataFrames.
"""

from .profiling import (
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

__version__ = "0.1.0" # Initial version

# Functions explicitly exported when using 'from polarspulse import *'
__all__ = [
    "profile",
    "column_type_ident",
    "column_missing_prop",
    "row_missing_prop",
    "column_dup_ind",
    "row_dup_ind",
    "num_stats",
    "num_outlier_stats",
    "cat_stats",
    "__version__"
]