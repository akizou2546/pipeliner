import os
import polars as pl
from dataclasses import dataclass, field
import sys
import os

def main(current_working_dir_path: str):
    print(current_working_dir_path)

    # ファイルの入力元・出力先とデータフレームを定義する
    paths = Paths(current_working_dir_path=current_working_dir_path)
    data = Data()

    # ファイルを読み込む
    reader = Reader(paths)
    data = reader.read_data()
    
    # テーブルデータを加工する
    processor = Processor(data)
    data = processor.process_data()

    # ファイルを出力する
    writer = Writer(paths, data)
    writer.write_data()

class Paths:
    def __init__(self, current_working_dir_path: str):
        self.input_dir = os.path.join(current_working_dir_path, "input")
        self.output_dir = os.path.join(current_working_dir_path, "output")

        self.input_sample_csv = os.path.join(self.input_dir, "sample.csv")

        self.output_id_value_csv = os.path.join(self.output_dir, "id_value.csv")

@dataclass
class Data:
    sample: pl.DataFrame = field(default_factory=pl.DataFrame)
    id_value: pl.DataFrame = field(default_factory=pl.DataFrame)

class Reader:
    def __init__(self, paths: Paths):
        self.paths = paths
        self.data = Data()
    def read_data(self):
        self.data.sample = Reader.read_sample(self.paths.input_sample_csv)
        print(self.data.sample.head())
        return self.data
    @staticmethod
    def read_sample(input_sample_csv: str):
        sample = pl.read_csv(input_sample_csv, encoding="cp932", infer_schema_length=0)
        return sample

    # def read_samples():
    #     samples = []
    #     for file_path in file_paths:
    #         # 各日付のファイルを読み込み、DataFrameをリストに追加
    #         dataframe = pl.read_csv(file_path)
    #         dataframes.append(dataframe)
    #     # 読み込んだDataFrameを結合して1つのDataFrameにする
    #     combined_dataframe = pl.concat(dataframes)
    #     return Data(combined_dataframe)

class Processor:
    def __init__(self, data: Data):
        self.data = data
    
    def process_data(self):
        self.group_by_id()
        print(self.data.id_value.head())
        return self.data
    
    def group_by_id(self):
        self.data.id_value = (
            self.data.sample
            .with_columns(
                [
                    pl.col("VALUE1").cast(float),
                    pl.col("VALUE2").cast(float)
                ]
            )
            .group_by(["ID"])
            .agg(
                [
                    pl.col("VALUE1").mean(),
                    pl.col("VALUE2").mean()
                ]
            )
            .sort(["ID"])
        )

        return self.data.id_value

class Writer:
    def __init__(self, paths: Paths, data: Data):
        self.paths = paths
        self.data = data

    def write_data(self):
        self.data.id_value.write_csv(self.paths.output_id_value_csv, include_bom=True)

# # チェーンパターンを使用して処理を連結
# class DataPipeline:
#     def __init__(self, data_directory):
#         # 各日付のデータが保存されているディレクトリパス
#         self.data_directory = data_directory
    
#     def process_data(self):
#         # 読み込むファイルのパスを取得
#         file_paths = [os.path.join(self.data_directory, file_name) for file_name in os.listdir(self.data_directory) if file_name.endswith(".csv")]
        
#         # データの読み込み
#         reader = DataReader()
#         data = reader.read_data(file_paths)
        
#         # データの前処理、処理、後処理を行う
#         UppercasePreprocessor.preprocess(data)
#         SpaceToUnderscoreProcessor.process(data)
#         LowercasePostprocessor.postprocess(data)
        
#         return data

