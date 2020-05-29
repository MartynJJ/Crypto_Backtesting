import tensorflow as tf
import pandas as pd

# assert int(tf.__version__[0]) >= 2

import_df = pd.read_csv('20171021_20171124_1min.csv')

print(import_df.index.dtype)
