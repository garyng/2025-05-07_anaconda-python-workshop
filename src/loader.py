from abc import ABC, abstractmethod
from collections import OrderedDict
from datetime import date
import os
import pathlib
from pydantic import BaseModel
import polars as pl
import pandera.polars as pa
import pandera.typing.polars as pat
import polars.testing.parametric as plt


class ReportLoader(ABC):
    @abstractmethod
    def execute(self):
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


class ReportSchema(RawReportSchema_ParsedFilename):
    @classmethod
    def to_schema(cls) -> pa.DataFrameSchema:
        schema = super().to_schema()
        return schema.remove_columns(["FinancialType"])


def filter(
    data: pat.DataFrame[RawReportSchema_ParsedFilename],
) -> pat.DataFrame[ReportSchema]:
    results = data.filter(pl.col("FinancialType").eq(pl.lit("Equities")))

    return ReportSchema.validate(results)


class FundCsvDirectoryReportLoader(ReportLoader):
    class Config(BaseModel):
        directory_path: str

    def __init__(self, config: Config):
        self.config = config

    def execute(self):
        raise NotImplementedError
