import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Define the chunk size
chunk_size = 1000  # Adjust chunk size based on your system's memory capacity
file_path = "/home/sushi/datascience/PupilBioTest_PMP_revA.csv"

# Initialize variables to store cumulative statistics
median_tissue1_list = []
cv_tissue1_list = []

# Example coverage column names (adjust according to your dataset)
coverage_columns_tissue1 = ['CpG_Coord1', 'CpG_Coord2', 'CpG_Coord3']

# Read and process data in chunks
chunks = pd.read_csv(file_path, chunksize=chunk_size)

for chunk in chunks:
    # Separate CpG Coordinates
    chunk[['CpG_Coord1', 'CpG_Coord2', 'CpG_Coord3']] = chunk['CpG_Coordinates'].str.split(':', n=2, expand=True)
    # Convert coordinates to numeric
    chunk['CpG_Coord1'] = chunk['CpG_Coord1'].astype(float)
    chunk['CpG_Coord2'] = chunk['CpG_Coord2'].astype(float)
    chunk['CpG_Coord3'] = chunk['CpG_Coord3'].astype(float)
    
    # Calculate Median and CV for Tissue #1
    median_tissue1 = chunk[coverage_columns_tissue1].median()
    mean_tissue1 = chunk[coverage_columns_tissue1].mean()
    std_tissue1 = chunk[coverage_columns_tissue1].std()
    cv_tissue1 = (std_tissue1 / mean_tissue1) * 100
    
    # Append statistics to lists
    median_tissue1_list.append(median_tissue1)
    cv_tissue1_list.append(cv_tissue1)

# Convert lists to DataFrame for plotting
median_tissue1_df = pd.concat(median_tissue1_list, axis=1).mean(axis=1)
cv_tissue1_df = pd.concat(cv_tissue1_list, axis=1).mean(axis=1)

# Combine the statistics into a single DataFrame for plotting
coverage_stats = pd.DataFrame({
    'Median_Tissue1': median_tissue1_df,
    'CV_Tissue1': cv_tissue1_df
})

# Set the display options to show all rows
pd.set_option('display.max_rows', None)
print(coverage_stats)

# Generate box plots for coverage distribution
fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 5))

for i, col in enumerate(coverage_columns_tissue1):
    sns.boxplot(data=chunk, y=col, ax=axes[i])
    axes[i].set_title(f'Coverage Distribution for CpG {i+1}')
    axes[i].set_ylabel('Coverage')

plt.tight_layout()
plt.savefig("coverage_box_plots.png")

# Generate histograms for coverage distribution
fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(15, 5))

for i, col in enumerate(coverage_columns_tissue1):
    sns.histplot(chunk[col], bins=30, kde=True, ax=axes[i])
    axes[i].set_title(f'Coverage Distribution for CpG {i+1}')
    axes[i].set_xlabel('Coverage')

plt.tight_layout()
plt.savefig("coverage_histograms.png")

# Plot median and CV values
plt.figure(figsize=(8, 5))
plt.plot(median_tissue1_df, label='Median Coverage')
plt.plot(cv_tissue1_df, label='Coefficient of Variation')
plt.xlabel('CpG Site')
plt.ylabel('Value')
plt.legend()
plt.title('Median and CV of Coverage')
plt.savefig("median_cv_plot.png")
