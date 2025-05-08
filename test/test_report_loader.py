import pathlib
from datetime import date

import polars as pl
import pytest

from loader import (
    RawReportSchema,
    get_csv_files_from_directory,
    load_csv_files,
    parse_csv_filename,
)
from test import test_data_dir_path


def test_can_get_csv_file_paths():
    paths = get_csv_files_from_directory(dir_path=test_data_dir_path)

    assert all(path.suffix == ".csv" for path in paths)


@pytest.mark.parametrize(
    "file_paths",
    [
        [test_data_dir_path / "Applebead.28-02-2023 breakdown.csv"],
        [
            test_data_dir_path / "Belaware.30_09_2022.csv",
            test_data_dir_path / "Report-of-Gohen.11-30-2022.csv",
            test_data_dir_path / "Virtous.05-31-2023 - securities.csv",
        ],
        pytest.param([], id="empty list"),
    ],
)
def test_can_load_csv_files(file_paths: list[pathlib.Path]):
    load_csv_files(file_paths)


@pytest.mark.parametrize(
    "filename,expected",
    [
        (
            "Applebead.28-02-2023 breakdown",
            {"FundName": "Applebead", "ReportDate": date(2023, 2, 28)},
        ),
        (
            "Belaware.28_02_2023",
            {"FundName": "Belaware", "ReportDate": date(2023, 2, 28)},
        ),
        (
            "Fund Whitestone.28-02-2023 - details",
            {"FundName": "Fund Whitestone", "ReportDate": date(2023, 2, 28)},
        ),
        ("Leeder.01_31_2023", {"FundName": "Leeder", "ReportDate": date(2023, 1, 31)}),
        (
            "mend-report Wallington.28_02_2023",
            {"FundName": "mend-report Wallington", "ReportDate": date(2023, 2, 28)},
        ),
        (
            "Report-of-Gohen.01-31-2023",
            {"FundName": "Report-of-Gohen", "ReportDate": date(2023, 1, 31)},
        ),
        (
            "rpt-Catalysm.2022-08-31",
            {"FundName": "rpt-Catalysm", "ReportDate": date(2022, 8, 31)},
        ),
        (
            "TT_monthly_Trustmind.20220831",
            {"FundName": "TT_monthly_Trustmind", "ReportDate": date(2022, 8, 31)},
        ),
        (
            "Virtous.01-31-2023 - securities",
            {"FundName": "Virtous", "ReportDate": date(2023, 1, 31)},
        ),
    ],
)
def test_can_parse_filenames(filename, expected):
    data = RawReportSchema.one_test_data().with_columns(Filename=pl.lit(filename))
    result = parse_csv_filename(data)

    assert result["FundName"].to_list() == [expected["FundName"]]
    assert result["ReportDate"].to_list() == [expected["ReportDate"]]
