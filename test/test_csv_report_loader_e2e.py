from polars.testing import assert_frame_equal

from config import load_app_config
from loader import FundCsvDirectoryReportLoader
from test import load_spec_from_csv, test_data_dir_path


def test_e2e_can_load_csv_fund_report(request):
    app_config = load_app_config()
    config = FundCsvDirectoryReportLoader.Config(
        directory_path=str(test_data_dir_path),
        fund_name_mappings=app_config.FundNameMappings,
    )
    loader = FundCsvDirectoryReportLoader(config)
    results = loader.execute()
    # save_result_as_spec_csv(request.node.name, results)
    expected = load_spec_from_csv(request.node.name)

    assert_frame_equal(results, expected)
