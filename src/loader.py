import os
import pathlib
from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import date
from typing import Generic, TypeVar

import pandera.polars as pa
import pandera.typing.polars as pat
import polars as pl
import polars.testing.parametric as plt
from pydantic import BaseModel

T = TypeVar("T")


class ReportLoader(ABC, Generic[T]):
    @abstractmethod
    def execute(self) -> Generic[T]:
        pass


def get_csv_files_from_directory(dir_path: str) -> list[pathlib.Path]:
    return [
        pathlib.Path(dir_path, f) for f in os.listdir(dir_path) if f.endswith(".csv")
    ]


class RawReportSchema(pa.DataFrameModel):
    class Config:
        strict = "filter"

    FinancialType: str
    Symbol: str
    SecurityName: str
    Price: float | None = pa.Field(nullable=True)
    Quantity: float | None = pa.Field(nullable=True)
    RealizedPnl: float | None = pa.Field(nullable=True)
    MarketValue: float | None = pa.Field(nullable=True)
    Filename: str

    @staticmethod
    def csv_mappings() -> dict[str, str]:
        return OrderedDict(
            {
                "FinancialType": "FINANCIAL TYPE",
                "Symbol": "SYMBOL",
                "SecurityName": "SECURITY NAME",
                "Price": "PRICE",
                "Quantity": "QUANTITY",
                "RealizedPnl": "REALISED P/L",
                "MarketValue": "MARKET VALUE",
            }
        )

    @staticmethod
    def one_test_data() -> pat.DataFrame["RawReportSchema"]:
        dfs = plt.dataframes(
            [
                plt.column("FinancialType", dtype=pl.String),
                plt.column("Symbol", dtype=pl.String),
                plt.column("SecurityName", dtype=pl.String),
                plt.column("Price", dtype=pl.Float64),
                plt.column("Quantity", dtype=pl.Float64),
                plt.column("RealizedPnl", dtype=pl.Float64),
                plt.column("MarketValue", dtype=pl.Float64),
                plt.column("Filename", dtype=pl.Float64),
            ],
            min_size=1,
            max_size=1,
            allow_null=False,
        )
        return dfs.example()


def load_csv_files(files: list[pathlib.Path]) -> pat.DataFrame[RawReportSchema]:
    results = [
        pl.read_csv(
            path,
            columns=list(RawReportSchema.csv_mappings().values()),
            new_columns=list(RawReportSchema.csv_mappings().keys()),
        ).with_columns(Filename=pl.lit(path.stem))
        for path in files
        if path.exists()
    ]

    if not results:
        return None

    result = pl.concat(results)
    return RawReportSchema.validate(result)


class RawReportSchema_ParsedFilename(RawReportSchema):
    FundName: str
    ReportDate: date

    @classmethod
    def to_schema(cls) -> pa.DataFrameSchema:
        schema = super().to_schema()
        return schema.remove_columns(["Filename"])


def parse_csv_filename(
    data: pat.DataFrame[RawReportSchema],
) -> pat.DataFrame[RawReportSchema_ParsedFilename]:
    parsed = (
        data.with_columns(
            ParsedFilename=pl.col(
                "Filename"
            ).str.extract_groups(r"""(?xi)  # verbose and case-insensitive
            (?P<FundName>.+?)
            \.
            (?P<ReportDateStr>
                (?:\d{8}) # YYYYMMDD
                |
                (?:\d{2}[-_]\d{2}[-_]\d{4}) # DD MM YYYY, MM DD YYYY, separators can be -_
                |
                (?:\d{4}[-_]\d{2}[-_]\d{2}) # YYYY MM DD, YYYY DD MM, separators can be -_
            )
        """)
        )
        .unnest("ParsedFilename")
        .drop("Filename")
    )

    date_formats = [
        "%m-%d-%Y",
        "%m_%d_%Y",
        "%d-%m-%Y",
        "%d_%m_%Y",
        "%Y%m%d",
        "%Y-%m-%d",
        "%Y_%m_%d",
    ]

    results = parsed.with_columns(
        ReportDate=pl.coalesce(
            [
                pl.col("ReportDateStr").str.strptime(pl.Date, f, strict=False)
                for f in date_formats
            ]
        )
    ).drop("ReportDateStr")

    return RawReportSchema_ParsedFilename.validate(results)


class ReportSchema(pa.DataFrameModel):
    class Config:
        strict = "filter"

    Symbol: str
    SecurityName: str
    Price: float
    Quantity: float
    RealizedPnl: float
    MarketValue: float

    FundName: str
    ReportDate: date


def filter_only_equities(
    data: pat.DataFrame[RawReportSchema_ParsedFilename],
) -> pat.DataFrame[ReportSchema]:
    results = data.filter(pl.col("FinancialType").eq(pl.lit("Equities")))

    return ReportSchema.validate(results)


class FundCsvDirectoryReportLoader(ReportLoader[pat.DataFrame[ReportSchema]]):
    class Config(BaseModel):
        directory_path: str

    def __init__(self, config: Config):
        self.config = config

    def execute(self) -> pat.DataFrame[ReportSchema]:
        csv_file_paths = get_csv_files_from_directory(self.config.directory_path)
        raw_data = load_csv_files(files=csv_file_paths)
        raw_data = parse_csv_filename(data=raw_data)
        data = filter_only_equities(data=raw_data)

        return data
