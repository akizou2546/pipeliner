import glob
import os
import sys
from dataclasses import dataclass, field

import folium
import geopandas as gpd
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
    def info(self):
        return os.path.join(self.model_dir, "info.csv")

    @property
    def sample(self):
        return os.path.join(self.model_dir, "sample.csv")

    @property
    def samples(self):
        return glob.glob(os.path.join(self.model_dir, "sample", "*_sample.csv"))

    @property
    def id_info_value(self):
        return os.path.join(self.output_dir, "id_info_value.csv")

    @property
    def id_info_value_geometry(self):
        return os.path.join(self.output_dir, "id_info_value_geometry.geojson")

    @property
    def map(self):
        return os.path.join(self.output_dir, "map.html")


@dataclass
class Data:
    # 入力
    id: pl.DataFrame = field(default_factory=pl.DataFrame)
    info: pl.DataFrame = field(default_factory=pl.DataFrame)
    sample: pl.DataFrame = field(default_factory=pl.DataFrame)
    samples: pl.DataFrame = field(default_factory=pl.DataFrame)

    # 出力
    id_info_value: pl.DataFrame = field(default_factory=pl.DataFrame)
    id_info_value_geometry: gpd.GeoDataFrame = field(default_factory=gpd.GeoDataFrame)


class Reader:
    @classmethod
    def read_data(cls, paths: Paths, data: Data):
        data.id = cls.read_excel(paths.id)
        print(data.id.head())
        data.info = cls.read_csv(paths.info)
        print(data.info.head())
        data.sample = cls.read_csv(paths.sample)
        print(data.sample.head())
        data.samples = cls.read_csvs(paths.samples, data.id)
        print(data.samples.head())
        return data

    @classmethod
    def read_excel(cls, excel_path: str):
        return pl.read_excel(
            excel_path,
            sheet_name="sample",
            read_options={"skip_rows": 5},
            infer_schema_length=0,
        )

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
        data.id_info_value = data.id.join(
            data.info.unique(["ID"]), how="left", on=["ID"]
        ).join(id_value, how="left", on=["ID"])
        print(data.id_info_value.head())
        # data.id_info_value_geometry = gpd.GeoDataFrame(
        #     data.id_info_value.to_pandas(),
        #     geometry=gpd.points_from_xy(
        #         data.id_info_value["LONG"].to_pandas(),
        #         data.id_info_value["LAT"].to_pandas(),
        #         crs="EPSG:3857", # crs="EPSG:6668" or crs="EPSG:4326" or crs="EPSG:3857"
        #     ),
        # )
        # print(data.id_info_value_geometry.head())
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
        cls.write_csv(data.id_info_value, paths.id_info_value)
        # cls.write_geojson(data.id_info_value_geometry, paths.id_info_value_geometry)
        # cls.write_map({"id": data.id_info_value_geometry}, paths.map, {"id": ["ID", "VALUE1"]})
        return None

    @classmethod
    def write_csv(cls, dataframe: pl.DataFrame, csv_path: str):
        return dataframe.write_csv(csv_path, include_bom=True)

    @classmethod
    def write_geojson(cls, geodataframe: gpd.GeoDataFrame, geojson_path: str):
        return geodataframe.to_file(
            geojson_path,
            encoding="utf-8",
            driver="GeoJSON",
            index=False,
            engine="pyogrio",
        )

    @classmethod
    def write_map(
        cls,
        geodataframes: dict[str, gpd.GeoDataFrame],
        map_path: str,
        fields: dict[str, list[str]],
    ):
        map = folium.Map(
            location=[
                list(geodataframes.values())[0]["geometry"]
                .to_crs("EPSG:3857")
                .centroid.y.mean(),
                list(geodataframes.values())[0]["geometry"]
                .to_crs("EPSG:3857")
                .centroid.x.mean(),
            ],
            zoom_start=10,
        )

        # GeoDataFrameを地図に追加する
        # 地図にGeoDataFrameを追加する
        for name, geodataframe in geodataframes.items():
            popup = folium.GeoJsonPopup(fields=fields[name])
            folium.GeoJson(geodataframe.to_json(), name=name, popup=popup).add_to(map)

        # レイヤー コントロールを追加する
        folium.LayerControl().add_to(map)

        map.save(map_path)
