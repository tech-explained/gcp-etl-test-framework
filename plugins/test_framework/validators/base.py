"""Base class for all validators."""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict

from ..context import ValidationContext
from ..models import TestResult, TestStatus


class BaseValidator(ABC):
    type_name: str = ""

    def __init__(self, test_case: Dict[str, Any], ctx: ValidationContext):
        self.tc = test_case
        self.ctx = ctx

    @abstractmethod
    def validate(self) -> TestResult:
        ...

    def _result(self, status: TestStatus, message: str = "", details: Dict[str, Any] = None) -> TestResult:
        return TestResult(
            test_id=self.tc["id"],
            name=self.tc.get("name", self.tc["id"]),
            validator=self.type_name,
            status=status,
            message=message,
            details=details or {},
        )
