import glob
import os
import sys
from dataclasses import dataclass, field

import polars as pl


def main():
    current_working_dir_path = os.getcwd()
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        current_working_dir_path = os.path.join(
            os.path.dirname(sys.argv[0]), os.path.pardir
        )
    print(current_working_dir_path)

    # ファイルの入力元・出力先とデータフレームを定義する
    paths = Paths(current_working_dir_path=current_working_dir_path)
    data = Data()

    # ファイルを読み込む
    data = Reader.read_data(paths, data)

    # テーブルデータを加工する
    data = Processor.process_data(data)

    # ファイルを出力する
    Writer.write_data(data, paths)


class Paths:
    def __init__(self, current_working_dir_path: str):
        self.current_working_dir = current_working_dir_path

    @property
    def input_dir(self):
        return os.path.join(self.current_working_dir, "input")

    @property
    def model_dir(self):
        return os.path.join(self.current_working_dir, "model")

    @property
    def output_dir(self):
        return os.path.join(self.current_working_dir, "output")

    @property
    def id(self):
        return os.path.join(self.input_dir, "id.xlsx")

    @property
    def sample(self):
        return os.path.join(self.model_dir, "sample.csv")

    @property
    def samples(self):
        return glob.glob(os.path.join(self.model_dir, "sample", "*_sample.csv"))

    @property
    def id_value(self):
        return os.path.join(self.output_dir, "id_value.csv")


@dataclass
class Data:
    # 入力
    id: pl.DataFrame = field(default_factory=pl.DataFrame)

    # 中間
    sample: pl.DataFrame = field(default_factory=pl.DataFrame)
    samples: pl.DataFrame = field(default_factory=pl.DataFrame)

    # 出力
    id_value: pl.DataFrame = field(default_factory=pl.DataFrame)


class Reader:
    @classmethod
    def read_data(cls, paths: Paths, data: Data):
        data.id = cls.read_excel(paths.id)
        print(data.id.head())
        data.sample = cls.read_csv(paths.sample)
        print(data.sample.head())
        data.samples = cls.read_csvs(paths.samples, data.id)
        print(data.samples.head())
        return data

    @classmethod
    def read_excel(cls, excel_path: str):
        return pl.read_excel(excel_path, sheet_name="sample", read_options={"skip_rows": 5}, infer_schema_length=0)

    @classmethod
    def read_csv(cls, csv_path: str):
        return pl.read_csv(csv_path, encoding="cp932", infer_schema_length=0)

    @classmethod
    def read_csvs(
        cls,
        csv_paths: list[str],
        id: pl.DataFrame,
        left_on: list[str] = ["ID"],
        right_on: list[str] = ["ID"],
    ):
        dataframe = pl.DataFrame()
        for csv_path in csv_paths:
            # 各日付のファイルを読み込み、DataFrameをリストに追加
            dataframe_csv = cls.read_csv(csv_path)
            dataframe_each = (
                id.select(left_on)
                .unique()
                .join(
                    dataframe_csv,
                    how="left",
                    left_on=left_on,
                    right_on=right_on,
                )
            )  # IDで絞り込む
            dataframe = pl.concat([dataframe, dataframe_each])
        return dataframe


class Processor:
    @classmethod
    def process_data(cls, data: Data):
        id_value = cls.group_by_id(data.sample)
        data.id_value = data.id.join(id_value, how="left", on=["ID"])
        print(data.id_value.head())
        return data

    @classmethod
    def group_by_id(cls, sample: pl.DataFrame):
        id_value = (
            sample.with_columns(
                [pl.col("VALUE1").cast(float), pl.col("VALUE2").cast(float)]
            )
            .group_by(["ID"])
            .agg([pl.col("VALUE1").mean(), pl.col("VALUE2").mean()])
            .sort(["ID"])
        )
        return id_value


class Writer:
    @classmethod
    def write_data(cls, data: Data, paths: Paths):
        cls.write_csv(data.id_value, paths.id_value)
        return None

    @classmethod
    def write_csv(cls, dataframe: pl.DataFrame, csv_path: str):
        return dataframe.write_csv(csv_path, include_bom=True)
