import os
import uuid
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path

from PyFlow import NSingletonDecorator


@NSingletonDecorator
class DataFrameManager(object):
    def __init__(self):
        self.data_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), "DataFrame")

    @staticmethod
    def get_file_name():
        return ''.join(str(uuid.uuid4()).split('-'))

    def get_file_path(self, filename):
        return Path(self.data_dir, filename)

    def write(self, dataframe, compression="lz4", preserve_index=True):
        filename = self.get_file_name()
        filepath = self.get_file_path(filename)
        table = pa.Table.from_pandas(dataframe, preserve_index=preserve_index)
        try:
            pq.write_table(table, filepath, compression=compression)
        except Exception as e:
            print(f"wtite data to {filepath} raise exception: {e}")
            return None
        return filename

    def read(self, filename):
        filepath = self.get_file_path(filename)
        if not filepath.exists():
            print(f"{filename} not exists in {self.data_dir}")
            return None
        table = pq.read_table(filepath, memory_map=True)
        return table.to_pandas()


if __name__ == '__main__':
    fpath = "C:\\Users\\yuepf01\\Documents\\workspace\\python\\PyFlow\\files\pengfei\\product_comp_info2.pkl"
    import pandas as pd

    data = pd.read_pickle(fpath, compression='xz')
    dfm = DataFrameManager()
    filename = dfm.write(data)
    df = dfm.read(filename)
