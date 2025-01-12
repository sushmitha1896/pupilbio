import dask.dataframe as dd
import pandas as pd
import numpy as np
from dask.distributed import Client

# Function to compute median and CV for a partition
def compute_stats(partition):
    coords = partition['CpG_Coordinates'].str.split(':', n=2, expand=True)
    partition['CpG_Coord1'] = coords[0].astype(float)
    partition['CpG_Coord2'] = coords[1].astype(float)
    partition['CpG_Coord3'] = coords[2].astype(float)
    
    coverage_columns_tissue1 = ['CpG_Coord1', 'CpG_Coord2', 'CpG_Coord3']
    
    median_tissue1 = partition[coverage_columns_tissue1].median()
    mean_tissue1 = partition[coverage_columns_tissue1].mean()
    std_tissue1 = partition[coverage_columns_tissue1].std()
    cv_tissue1 = (std_tissue1 / mean_tissue1) * 100

    return pd.DataFrame({
        'median_tissue1': median_tissue1,
        'cv_tissue1': cv_tissue1
    })

# Main execution block
if __name__ == '__main__':
    # Setup Dask client for parallel processing
    client = Client(n_workers=4, threads_per_worker=2)  # Adjust based on your CPU cores

    # Load data from CSV file using Dask
    file_path = "/home/sushi/datascience/PupilBioTest_PMP_revA.csv"
    data = dd.read_csv(file_path, blocksize='10MB')  # Smaller block size

    # Print the column names to verify
    print("Available columns:", data.columns)

    # Define expected metadata
    meta = pd.DataFrame({
        'median_tissue1': pd.Series(dtype='float64'),
        'cv_tissue1': pd.Series(dtype='float64')
    })

    # Apply function to each partition and compute the results
    results = data.map_partitions(compute_stats, meta=meta).compute()
    pd.set_option('display.max_rows', None)
    print(results)
