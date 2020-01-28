from notebooks.cohd_requests import *

# Check that
df_dataset = datasets()
assert df_dataset is not None and df_dataset.shape[0] >= 3
