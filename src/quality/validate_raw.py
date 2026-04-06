import logging
import sys
from datetime import datetime, timezone

import great_expectations as gx
from great_expectations.core import ExpectationConfiguration


def setup_datasource(
    context, datasource_name, datasource_dir, asset_name, batching_regex
):
    datasource = context.sources.add_or_update_pandas_filesystem(
        name=datasource_name, base_directory=datasource_dir
    )
    if asset_name not in datasource.assets:
        datasource.add_csv_asset(
            name=asset_name,
            batching_regex=batching_regex,
            header=0,
            sep=",",
        )
    return datasource


def setup_expectation_suite(context, name):
    suite = context.add_or_update_expectation_suite(name)

    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "id_basica"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "nm_empresa"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "nr_voo", "min_value": 1, "max_value": 9999},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "nr_assentos_ofertados",
                "min_value": 10,
                "max_value": 800,
            },
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_strftime_format",
            kwargs={"column": "dt_referencia", "strftime_format": "%Y-%m-%d"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_strftime_format",
            kwargs={"column": "dt_partida_real", "strftime_format": "%Y-%m-%d"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_match_strftime_format",
            kwargs={"column": "dt_chegada_real", "strftime_format": "%Y-%m-%d"},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={"column": "kg_payload", "min_value": 0, "max_value": 50000},
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "km_distancia",
                "min_value": 0,
                "max_value": 10000,
            },
        )
    )

    context.add_or_update_expectation_suite(expectation_suite=suite)
    return suite


def run_checkpoint(
    context, checkpoint_name, batch_request, expectation_suite_name
):
    checkpoint = context.add_or_update_checkpoint(
        name=checkpoint_name,
        validations=[
            {
                "batch_request": batch_request,
                "expectation_suite_name": expectation_suite_name,
            }
        ],
    )
    run_name = datetime.now(tz=timezone.utc).strftime("anac_%Y%m%d__%H_%M_%S")
    return checkpoint.run(run_name=run_name, result_format="SUMMARY")


def log_results(context, results):
    for key, run_result in results.run_results.items():
        stats = run_result["validation_result"]["statistics"]
        evaluated = stats["evaluated_expectations"]
        unsuccessful = stats["unsuccessful_expectations"]

        for expectation_result in run_result["validation_result"]["results"]:
            expectation_type = expectation_result["expectation_config"][
                "expectation_type"
            ]
            column = expectation_result["expectation_config"]["kwargs"].get(
                "column", ""
            )
            status = "PASS" if expectation_result["success"] else "FAIL"
            logging.info(f"  [{status}] {expectation_type}({column})")

        for url in context.get_docs_sites_urls(resource_identifier=key):
            logging.info(
                f"Validation result: {url['site_url'].replace('%5C', '/')}"
            )

        if not results.success:
            logging.error(
                f"Validação falhou: {unsuccessful}/{evaluated} expectations não atendidas."
            )
            sys.exit(1)
        logging.info(
            f"Validação concluída com sucesso: {evaluated - unsuccessful}/{evaluated} expectations atendidas."
        )


def run(config: dict):
    logging.info("Starting raw data validation...")
    raw_path = config["raw"]["path"]
    expectation_suite_name = config["quality"]["expectation_suite_name"]
    asset_name = config["quality"]["asset_name"]
    datasource_name = config["quality"]["datasource_name"]
    checkpoint_name = config["quality"]["checkpoint_name"]

    context = gx.get_context(project_root_dir="./gx")

    datasource = setup_datasource(
        context, datasource_name, raw_path, asset_name, r".*\.csv"
    )
    batch_request = datasource.get_asset(asset_name).build_batch_request()

    setup_expectation_suite(context, expectation_suite_name)
    results = run_checkpoint(
        context, checkpoint_name, batch_request, expectation_suite_name
    )

    context.build_data_docs()
    log_results(context, results)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    run(
        {
            # "raw": {"path": "data/raw/sample/"},
            "raw": {"path": "data/raw/"},
            "quality": {
                "expectation_suite_name": "anac_expectation_suite",
                "asset_name": "my_asset",
                "datasource_name": "my_pandas_datasource",
                "checkpoint_name": "anac_checkpoint",
            },
        }
    )
