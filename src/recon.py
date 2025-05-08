import sqlite3
from abc import ABC
from datetime import date
from typing import Generic, TypeVar

import pandera.polars as pa
import pandera.typing.polars as pat
import polars as pl
from pydantic import BaseModel

from loader import FundReportSchema


def connect_db(db_path: str) -> sqlite3.Connection:
    return sqlite3.connect(db_path)


class EquityPriceSchema(pa.DataFrameModel):
    DateTime: date
    Symbol: str
    Price: float

    @staticmethod
    def columns_mapping():
        return {"DATETIME": "DateTime", "SYMBOL": "Symbol", "PRICE": "Price"}


def load_equity_prices_from_db(
    conn: sqlite3.Connection,
) -> pat.DataFrame[EquityPriceSchema]:
    results = (
        pl.read_database(query="select * from equity_prices", connection=conn)
        .with_columns(DATETIME=pl.col("DATETIME").str.strptime(pl.Date, "%m/%d/%Y"))
        .rename(EquityPriceSchema.columns_mapping())
    )

    return EquityPriceSchema.validate(results)


class EquityReconReportSchema(pa.DataFrameModel):
    FundName: str
    Symbol: str
    ReportDate: date
    ReportPrice: float
    RefPrice: float


def recon_ref_with_reports(
    ref_data: pat.DataFrame[EquityPriceSchema],
    report_data: pat.DataFrame[FundReportSchema],
) -> pat.DataFrame[EquityReconReportSchema]:
    results = (
        report_data.sort(["Symbol", "ReportDate"])
        .rename(lambda x: f"l.{x}")
        .join_asof(
            ref_data.sort(["Symbol", "DateTime"]).rename(lambda x: f"r.{x}"),
            strategy="backward",
            by_left=["l.Symbol"],
            left_on="l.ReportDate",
            by_right=["r.Symbol"],
            right_on="r.DateTime",
        )
        .filter(pl.col("l.Price").eq(pl.col("r.Price")).not_())
        .select(
            FundName=pl.col("l.FundName"),
            Symbol=pl.col("l.Symbol"),
            ReportDate=pl.col("l.ReportDate"),
            ReportPrice=pl.col("l.Price"),
            RefPrice=pl.col("r.Price"),
        )
    )

    return EquityReconReportSchema.validate(results)


TFundReport = TypeVar("T")
UReconReport = TypeVar("U")


class ReconReportGenerator(ABC, Generic[TFundReport, UReconReport]):
    def generate(self, report_data: TFundReport) -> UReconReport:
        pass


class EquityPriceReconReportGenerator(
    ReconReportGenerator[
        pat.DataFrame[FundReportSchema], pat.DataFrame[EquityReconReportSchema]
    ]
):
    class Config(BaseModel):
        connection_string: str

    def __init__(self, config: Config):
        self.config = config

    def generate(
        self, report_data: pat.DataFrame[FundReportSchema]
    ) -> pat.DataFrame[EquityReconReportSchema]:
        db = connect_db(self.config.connection_string)
        ref_data = load_equity_prices_from_db(conn=db)
        results = recon_ref_with_reports(ref_data=ref_data, report_data=report_data)
        return results
