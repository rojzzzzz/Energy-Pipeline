from dataclasses import dataclass, field
from typing import Any

@dataclass(frozen=True)
class Column:
    name: str
    type: str
    nullable: bool = True

@dataclass(frozen=True)
class Expectation:
    type: str
    kwargs: dict[str, Any] = field(default_factory=dict)

@dataclass(frozen=True)
class SourceContract:
    name: str
    dimensions: dict[str, Any] = field(default_factory=dict)
    expectations: list[Expectation] = field(default_factory=list)
    schema: list[Column] = field(default_factory=list)
    keys: list[str] = field(default_factory=list)

@dataclass(frozen=True)
class DimensionResult:
    source: str
    dimension: str
    pct: float
    threshold: float

    @property
    def passed(self) -> bool:
        return self.pct >= self.threshold

@dataclass(frozen=True)
class QualityResults:
    source: str
    uniqueness_result: DimensionResult
    completeness_result: DimensionResult
    expectation_passed: bool = False

    @property
    def passed(self) -> bool:
        return self.uniqueness_result.passed and self.completeness_result.passed and self.expectation_passed


@dataclass
class Contract:
    sources: list[SourceContract] = field(default_factory=list)
    results: list[QualityResults] = field(default_factory=list)

    @property
    def accomplished(self) -> bool:
        source_names = [source.name for source in self.sources]
        result_names = [result.source for result in self.results]
        return (bool(source_names) and len(source_names) == len(set(source_names))
                and sorted(result_names) == sorted(source_names) and
                all(result.passed for result in self.results))