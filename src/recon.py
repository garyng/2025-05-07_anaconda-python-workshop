import sqlite3
from datetime import date

import pandera.polars as pa
import pandera.typing.polars as pat
import polars as pl

from loader import ReportSchema


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
    ref_data: pat.DataFrame[EquityPriceSchema], report_data: pat.DataFrame[ReportSchema]
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
