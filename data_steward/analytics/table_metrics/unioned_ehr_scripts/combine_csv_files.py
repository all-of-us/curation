import xlsxwriter
import re

import pandas as pd
import os
import glob
path = "../unioned_ehr_scripts"
all_csv_files = glob.glob(os.path.join(path, "*.csv"))
df_from_each_file = (pd.read_csv(f) for f in all_csv_files)

# +
#The file name convention is like "july_13_2020"
from datetime import datetime

now = datetime.now()
file_name = now.strftime("%B_%d_%Y").lower()
file_name

# +
#file_name = 'august_03_2020'
# -

all_csv_files.sort()
all_csv_files

pattern = "(.*?).csv"

with pd.ExcelWriter('{}.xlsx'.format(file_name)) as writer:
    for f in all_csv_files:
        df = pd.read_csv(f, index_col = 0)
        df.to_excel(writer, sheet_name=re.search(pattern, os.path.basename(f)).group(1))
#writer.save()


