from great_expectations.expectations import ExpectColumnValuesToNotBeNull, ExpectCompoundColumnsToBeUnique, \
ExpectColumnValuesToBeUnique
from config import DATA_CONTRACT_PATH, VALIDATION_PATH
from datetime import date, datetime
import great_expectations as gx
from .classses import *
import pandas as pd
import logging
import json

logger = logging.getLogger(__name__)

def load_contract(sources_contract: dict) -> Contract:
    """Using the classes created in classes.py, it reads the contract and returns a Contract object containing
    multiple Sources objects"""

    sources = []
    for format, datasets in sources_contract.items():
        for source_contract in datasets:
            silver = source_contract['silver']
            schema = silver['schema']
            keys = silver['keys']
            policy = silver['quality']

            columns = [Column(name=col['name'], type=col['type'], nullable=col.get('nullable', True)) for col in schema]
            dimensions = {'completeness': policy['completeness'], 'uniqueness': policy['uniqueness']}

            expectations = [Expectation(type=exp['type'], kwargs=exp['kwargs']) for exp in policy['expectations']]

            sources.append(SourceContract(
                name=source_contract['name'],
                dimensions=dimensions,
                expectations=expectations,
                schema=columns,
                keys=keys,
            ))

    logger.info(f"Loaded contract with {len(sources)} sources")
    return Contract(sources=sources)

def measure_completeness(df: pd.DataFrame, source: SourceContract) -> DimensionResult:
    completeness_contract =source.dimensions['completeness']
    cols = completeness_contract.get('columns')
    if cols:
        df = df[cols].copy()

    completeness_pct = df.notna().to_numpy().sum() / df.size
    logger.info(f"Completeness for source {source.name}: {completeness_pct*100}% (threshold: {completeness_contract.get('threshold', 0)*100}%)")
    return DimensionResult(
        source=source.name,
        dimension='completeness',
        pct=completeness_pct,
        threshold=completeness_contract.get('threshold', 0)
    )

def measure_uniqueness(df: pd.DataFrame, source: SourceContract) -> DimensionResult:
    if not source.keys:
        logger.warning(f"No keys defined for source {source.name}. Uniqueness cannot be measured.")
        return DimensionResult(
            source=source.name,
            dimension='uniqueness',
            pct=100.0,
            threshold=100.0
        )

    duplicated_n = df.duplicated(subset=source.keys, keep=False).sum()
    not_duplicated_n = len(df) - duplicated_n
    uniqueness_pct = not_duplicated_n / len(df)
    threshold = source.dimensions['uniqueness'].get('threshold', 0)

    logger.info(f"Uniqueness for source {source.name}: {uniqueness_pct*100}% (threshold: {threshold*100}%)")
    return DimensionResult(
        source=source.name,
        dimension='uniqueness',
        pct=uniqueness_pct,
        threshold=threshold
    )

def expectation_class(expectation_type: str):
    """Converts the expectations declared in the data contract to actual gx expectation classes"""

    class_name = "".join(part.capitalize() for part in expectation_type.split("_"))
    try:
        return getattr(gx.expectations, class_name)
    except AttributeError:
        logger.error(f"Expectation class {class_name} not found in great_expectations.expectations")
        raise ValueError(f"Expectation class {class_name} not found in great_expectations.expectations")

def build_suite(source: SourceContract) -> gx.ExpectationSuite:

    suite = gx.ExpectationSuite(name=f"{source.name}-suite")
    expected_cols = [col.name for col in source.schema]
    suite.add_expectation(gx.expectations.ExpectTableColumnsToMatchSet(
        column_set=expected_cols))

    for col in source.schema:
        if not col.nullable or col.name in source.keys:
            suite.add_expectation(ExpectColumnValuesToNotBeNull(column=col.name))

    if source.keys:
        if len(source.keys) == 1:
            suite.add_expectation(ExpectColumnValuesToBeUnique(column=source.keys[0]))
            suite.add_expectation(ExpectCompoundColumnsToBeUnique(column_list=source.keys))
        else:
            suite.add_expectation(ExpectCompoundColumnsToBeUnique(column_list=source.keys))

    for exp in source.expectations:
        expect_class = expectation_class(exp.type)
        suite.add_expectation(expect_class(**exp.kwargs))

    return suite

def get_quality_results(source: str, uniqueness_result: DimensionResult, completeness_result: DimensionResult, expectation_passed: bool) -> QualityResults:
    """Helper function to create a QualityResults object from the validation results"""

    return QualityResults(
        source=source,
        uniqueness_result=uniqueness_result,
        completeness_result=completeness_result,
        expectation_passed=expectation_passed
    )

def create_resume(contract: Contract):
    
    for result in contract.results:
        source_resume = {
            "source": result.source,
            "validation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "uniqueness": {
                "pct": result.uniqueness_result.pct,
                "threshold": result.uniqueness_result.threshold,
                "passed": bool(result.uniqueness_result.passed)
            },
            "completeness": {
                "pct": result.completeness_result.pct,
                "threshold": result.completeness_result.threshold,
                "passed": bool(result.completeness_result.passed)
            },
            "expectations_passed": result.expectation_passed,
            "overall_passed": bool(result.passed)
        }

        with open(VALIDATION_PATH / f"{result.source}_validation_resume.json", "w") as f:
            json.dump(source_resume, f, indent=4)

def run_validation(dataframes: dict[str, pd.DataFrame], sources_contract: dict[str, list[dict]]) -> Contract:
    """Runs validation on the dataframes based on the sources contract and returns a Contract object with the results"""

    logger.info(f"Starting validation for {len(dataframes)} dataframes")

    contract = load_contract(sources_contract)
    source_names = [source.name for source in contract.sources]

    if len(source_names) != len(set(source_names)):
        logger.error("Duplicate source names found in the contract")
        raise ValueError("Duplicate source names found in the contract")

    context = gx.get_context()
    datasource = context.data_sources.add_pandas(name='silver')
    results = []

    for source in contract.sources:
        df = dataframes.get(source.name)
        if not isinstance(df, pd.DataFrame):
            logger.error(f"Staged data for {source.name} must be a pandas DataFrame")
            raise TypeError(f"Staged data for {source.name} must be a pandas DataFrame")

        if df.empty:
            logger.error(f"Cannot validate source {source.name}: the dataframe is empty")
            raise ValueError(f"Cannot validate source {source.name}: the dataframe is empty")

        logger.info(f"Starting validation for source {source.name}: {len(df)} rows, {len(df.columns)} columns")

        for col in source.schema:
            if col.name not in df.columns:
                logger.error(f"Column {col.name} is missing in the dataframe for source {source.name}")
                raise ValueError(f"Column {col.name} is missing in the dataframe for source {source.name}")

            values = df[col.name]
            kind = col.type
            valid_type = {
                'string': lambda: values.dropna().map(lambda value: isinstance(value, str)).all(),
                'int': lambda: pd.api.types.is_integer_dtype(values.dtype),
                'float': lambda: pd.api.types.is_numeric_dtype(values.dtype)
                                and not pd.api.types.is_bool_dtype(values.dtype),
                'bool': lambda: pd.api.types.is_bool_dtype(values.dtype),
                'datetime': lambda: pd.api.types.is_datetime64_any_dtype(values.dtype),
                'date': lambda: values.dropna().map(lambda value: type(value) is date).all(),
            }

            if kind not in valid_type or not valid_type[kind]():
                logger.error(f"Invalid type for source {source.name}, column {col.name}: expected {kind}, received {values.dtype}")
                raise TypeError(f"Prepare {source.name}.{col.name} as {kind} before validation")

        logger.info(f"Schema and type checks passed for source {source.name}")
        completeness_results = measure_completeness(df, source)
        uniqueness_results = measure_uniqueness(df, source)

        asset = datasource.add_dataframe_asset(name=source.name)
        batch = asset.add_batch_definition_whole_dataframe(name=f'{source.name}-batch')
        suite = context.suites.add(build_suite(source))
        validation = context.validation_definitions.add(gx.ValidationDefinition(
            name=f'{source.name}-validation', data=batch, suite=suite))
        logger.info(f"Running expectations for source {source.name}")
        suite_result = validation.run(batch_parameters={'dataframe': df})

        quality_results = get_quality_results(
            source=source.name,
            uniqueness_result=uniqueness_results,
            completeness_result=completeness_results,
            expectation_passed=suite_result.success
        )
        results.append(quality_results)

        if not suite_result.success:
            for check in suite_result.results:
                if not check.success:
                    logger.error(f"{check.expectation_config.type} failed for {source.name}: {check.result}")

        logger.info(f"Validation completed for source {source.name}. Passed: {quality_results.passed}. Uniqueness: {uniqueness_results.pct}%, Completeness: {completeness_results.pct}%")

    contract.results = results
    create_resume(contract)
    logger.info(f"Validation completed for all sources. Overall passed: {contract.accomplished}")
    return contract
