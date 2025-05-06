## Test the profiling of a large dataset with random data issues

import polars as pl
import numpy as np
import random
import string
from datetime import date, timedelta
from math import nan
import time

def insert_rand_data_issue(val, missing_issue_prop=0.1, nan_issue_prop=0.05, measure_issue_prop=0.01, measure_issue_val=-99):
    """
    A function to randomly insert missing, nan, or measurement issues in data
    """
    # random number between 0 and 1
    u = np.random.uniform()

    # Set proportion-based thresholds for missing, nan, and measurement issues  
    u_t1 = missing_issue_prop 
    u_t2 = u_t1 + nan_issue_prop
    u_t3 = u_t2 + measure_issue_prop
    out = val

    # Apply random issue
    if u <= u_t1:
        out = None
    elif  u > u_t1 and u <= u_t2:
        out = nan
    elif u > u_t2 and u <= u_t3:
        out = measure_issue_val
    else:
        out = val

    return out

def sim_data(n:int, p_cat_features:int, p_num_features:int, seed:int, data_issues:bool) -> pl.DataFrame:
    """
    A function to simulate data with random data issues.
    """

    # Setup Generator with seed
    rng = np.random.default_rng(seed)

    # Random n x p numeric and catagorical variable generations
    x_num = rng.standard_normal(size=(n, p_num_features))
    x_cat= rng.binomial(n=5, p=0.1, size=(n, p_cat_features))
    # x_cat_str=rng.choice(["A", "B", "C", "D"], replace=True, p=[0.3, 0.3, 0.3, 0.1], size=(n, p_cat_features))

    # Random day generation
    # Ref: https://www.geeksforgeeks.org/python-generate-k-random-dates-between-two-other-dates/
    start_date, end_date = date(2005, 1, 1), date(2025, 1, 1)
    dates_diff = end_date - start_date
    total_days = dates_diff.days

    rand_days_offset = rng.choice(total_days, size=n, replace=True)
    date_var = [start_date + timedelta(days=int(day)) for day in rand_days_offset]

    # Random text generation
    # Ref: https://www.geeksforgeeks.org/python-generate-random-string-of-given-length/
    text_var = [''.join(random.choices(string.ascii_lowercase, k=10)) for i in range(n)]

    df= pl.concat([
        pl.from_numpy(x_num, schema=[f"x{i + 1}" for i in range(p_num_features)]),
        pl.from_numpy(x_cat, schema=[f"g{i + 1}" for i in range(p_cat_features)]),
        pl.DataFrame({"datetime":date_var}),
        pl.DataFrame({"message":text_var})
        ], how="horizontal")

    # Gen random data issues
    if data_issues:
        df = df.with_columns(
                # Add random missing, nan, and outlier values
                *(pl.col(c).map_elements(lambda x: insert_rand_data_issue(x),
                            return_dtype=pl.Float64) for c in df.select(pl.col(pl.Float64)).columns),
                
                # add two columns with null types (i.e. all empty)
                pl.lit(None).alias("nullcol"), 
                pl.lit(float("nan")).alias("NANcol")
                )
        
        # add 1% empty rows for all columns
        df = df.vstack(pl.DataFrame([[None]*int(0.01*n) for x in range(df.width)], schema=df.schema))
            # Note This can be done with "extend" but the method modifies the dataframe in-place
        
        # add a duplicate column
        df = df.with_columns(DupCol=pl.col(df.columns[0]))

        # add a duplicate row (last step*)
        df = df.vstack(df[0])

    else: 
        df=df

    return df

# Simulate data with 1 million rows, 50 categorical features,50 numeric features, and include data issues
df = sim_data(n=1_000_000, p_cat_features=50, p_num_features=50, seed=1, data_issues=False)

start_time = time.time()
# Profile the data
df_profile, df_col_profile, df_row_profile = profile(df)
end_time = time.time()
print(f"Profiling time: {end_time - start_time} seconds")
# Profiling time: 71.58008193969727 seconds
# system: 64-bit Windows 10, 16GB RAM, Intel i7-6820HQ CPU @ 2.70GHz
# Note: Profiling time may vary based on system performance and data size.


