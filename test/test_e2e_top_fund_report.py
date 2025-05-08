from polars.testing import assert_frame_equal

from reports.top_fund import TopFundByMonthReportGenerator
from test import load_spec_from_csv, load_test_fund_report_data


def test_e2e_can_generate_top_fund_report(request):
    fund_report_data = load_test_fund_report_data()
    recon_generator = TopFundByMonthReportGenerator()
    results = recon_generator.generate(fund_report_data)
    # save_result_as_spec_csv(request.node.name, results)
    expected = load_spec_from_csv(request.node.name)
    assert_frame_equal(results, expected)
