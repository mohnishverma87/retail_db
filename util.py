import pandas as pd

def get_tables(path):
    tables = pd.read_csv(path, sep=':')
    return tables[tables.iloc[:,1] == 'yes']



