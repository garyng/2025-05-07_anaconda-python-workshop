from datetime import date
import pandera.polars as pa
import pandera.typing.polars as pat
import polars as pl

from loader import FundReportSchema
from reports import ReportGenerator


class TopFundByMonthReportSchema(pa.DataFrameModel):
    Date: str
    TopFundName: str
    RateOfReturn: float


@pa.check_output(TopFundByMonthReportSchema)
def generate_top_fund_by_month(
    fund_report_data: pat.DataFrame[FundReportSchema],
) -> pat.DataFrame[TopFundByMonthReportSchema]:
    ror_data = (
        fund_report_data.sort("FundName", "ReportDate")
        .group_by("FundName", "ReportDate", maintain_order=True)
        .agg(
            MarketValueSum=pl.sum("MarketValue"),
            RealizedPnlSum=pl.sum("RealizedPnl"),
        )
        .with_columns(
            PrevMarketValueSum=pl.col("MarketValueSum").shift().over("FundName")
        )
        .with_columns(
            RateOfReturn=(
                pl.col("MarketValueSum")
                - pl.col("PrevMarketValueSum")
                + pl.col("RealizedPnlSum")
            )
            / pl.col("PrevMarketValueSum")
        )
        .filter(pl.col("RateOfReturn").is_not_null())
    )

    results = (
        ror_data.sort("ReportDate")
        .group_by("ReportDate", maintain_order=True)
        .agg(
            # get the highest ror
            pl.all().sort_by("RateOfReturn").last()
        )
        .select(
            Date=pl.col("ReportDate").dt.strftime("%Y-%m"),
            TopFundName="FundName",
            RateOfReturn="RateOfReturn",
        )
    )

    return results


class TopFundByMonthReportGenerator(
    ReportGenerator[FundReportSchema, pat.DataFrame[TopFundByMonthReportSchema]]
):
    def generate(
        self, fund_report_data: pat.DataFrame[FundReportSchema]
    ) -> pat.DataFrame[TopFundByMonthReportSchema]:
        return generate_top_fund_by_month(fund_report_data=fund_report_data)
