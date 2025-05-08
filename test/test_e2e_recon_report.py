import pandera.typing.polars as pat
from polars.testing import assert_frame_equal

from loader import FundCsvDirectoryReportLoader, FundReportSchema
from recon import EquityPriceReconReportGenerator
from test import load_spec_from_csv, test_data_dir_path, test_db_path


def load_test_fund_report_data() -> pat.DataFrame[FundReportSchema]:
    config = FundCsvDirectoryReportLoader.Config(
        directory_path=str(test_data_dir_path),
        fund_name_mappings={},
    )
    loader = FundCsvDirectoryReportLoader(config)
    results = loader.execute()
    return results


def test_e2e_can_generate_recon_report(request):
    fund_report_data = load_test_fund_report_data()
    recon_generator = EquityPriceReconReportGenerator(
        EquityPriceReconReportGenerator.Config(connection_string=str(test_db_path))
    )
    results = recon_generator.generate(fund_report_data)
    # save_result_as_spec_csv(request.node.name, results)
    expected = load_spec_from_csv(request.node.name)
    assert_frame_equal(results, expected)
