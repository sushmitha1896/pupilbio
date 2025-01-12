import dask.dataframe as dd

# Load data from CSV file using Dask
file_path = "/home/sushi/datascience/PupilBioTest_PMP_revA.csv"
data = dd.read_csv(file_path)

# Print the column names to verify
print("Available columns:", data.columns)

# Extracting VRF column with the name `000`
vrf_tissue1 = data["`000"]
vrf_tissue2 = data["`001"]
vrf_tissue3 = data["`010"]
vrf_tissue4 = data["`011"]
vrf_tissue5 = data["`100"]
vrf_tissue6 = data["`101"]
vrf_tissue7 = data["`111"]

# Calculate the mean VRF for Tissue 1
mean_vrf_PMP1 = vrf_tissue1.mean().compute()
mean_vrf_PMP2 = vrf_tissue2.mean().compute()
mean_vrf_PMP3 = vrf_tissue3.mean().compute()
mean_vrf_PMP4 = vrf_tissue4.mean().compute()
mean_vrf_PMP5 = vrf_tissue5.mean().compute()
mean_vrf_PMP6 = vrf_tissue6.mean().compute()
mean_vrf_PMP7 = vrf_tissue7.mean().compute()




print("Mean VRF in PMP 1:", mean_vrf_PMP1)
print("Mean VRF in PMP 2:", mean_vrf_PMP2)
print("Mean VRF in PMP 3:", mean_vrf_PMP3)
print("Mean VRF in PMP 4:", mean_vrf_PMP4)
print("Mean VRF in PMP 5:", mean_vrf_PMP5)
print("Mean VRF in PMP 6:", mean_vrf_PMP6)
print("Mean VRF in PMP 7:", mean_vrf_PMP7)


