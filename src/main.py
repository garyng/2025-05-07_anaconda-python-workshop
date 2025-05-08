from config import AppConfig, load_app_config
from loader import FundCsvDirectoryReportLoader
from reports.recon import EquityPriceReconReportGenerator
from reports.top_fund import TopFundByMonthReportGenerator


def generate_recon_report(app_config: AppConfig):
    fund_report_loader = FundCsvDirectoryReportLoader(
        FundCsvDirectoryReportLoader.Config(
            directory_path=app_config.FundReportsDirPath,
            fund_name_mappings=app_config.FundNameMappings,
        )
    )
    fund_report_data = fund_report_loader.execute()
    recon_generator = EquityPriceReconReportGenerator(
        EquityPriceReconReportGenerator.Config(
            connection_string=app_config.RefDbConnectionString
        )
    )
    recon_report_data = recon_generator.generate(fund_report_data)
    recon_report_data.write_csv(app_config.ReconReportOutputPath)


def generate_top_fund_report(app_config: AppConfig):
    fund_report_loader = FundCsvDirectoryReportLoader(
        FundCsvDirectoryReportLoader.Config(
            directory_path=app_config.FundReportsDirPath,
            fund_name_mappings=app_config.FundNameMappings,
        )
    )
    fund_report_data = fund_report_loader.execute()
    top_fund_generator = TopFundByMonthReportGenerator()
    top_fund_report_data = top_fund_generator.generate(
        fund_report_data=fund_report_data
    )
    top_fund_report_data.write_csv(app_config.TopFundReportOutputPath)


if __name__ == "__main__":
    app_config = load_app_config()
    generate_recon_report(app_config=app_config)
    generate_top_fund_report(app_config=app_config)