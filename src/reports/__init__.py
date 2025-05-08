from abc import ABC, abstractmethod
from typing import Generic, TypeVar


TFundReport = TypeVar("T")
TReport = TypeVar("U")


class ReportGenerator(ABC, Generic[TFundReport, TReport]):
    @abstractmethod
    def generate(self, fund_report_data: TFundReport) -> TReport:
        pass
