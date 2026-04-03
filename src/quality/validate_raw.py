import great_expectations as gx

from src.processing.schema import SCHEMA_SPARK


def setup_datasource(
    context,
    datasource_name,
    datasource_dir,
    asset_name,
    batching_regex="*",
    schema=None,
):
    datasource = context.sources.add_pandas_filesystem(
        name=datasource_name, base_directory=datasource_dir
    )
    datasource_config = {
        "name": asset_name,
        "batching_regex": batching_regex,
        "header": True,
        "sep": ",",
    }
    # if schema is not None:
    #     datasource_config["schema"] = schema
    # else:
    #     datasource_config["infer_schema"] = True

    datasource.add_csv_asset(**datasource_config)
    return datasource


def setup_validator(context, batch_request, name="my_expectation_suite"):
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite_name=name,
    )
    validator.expect_column_values_to_not_be_null("id_basica")
    validator.expect_column_values_to_not_be_null("nm_empresa")
    validator.expect_column_values_to_be_between(
        "nr_voo", min_value=1, max_value=9999
    )
    validator.expect_column_values_to_be_between(
        "nr_assentos_ofertados", min_value=10, max_value=800
    )
    validator.expect_column_values_to_match_strftime_format(
        "dt_referencia", "%Y-%m-%d"
    )
    validator.expect_column_values_to_match_strftime_format(
        "dt_partida_real", "%Y-%m-%d"
    )
    validator.expect_column_values_to_match_strftime_format(
        "dt_chegada_real", "%Y-%m-%d"
    )
    validator.expect_column_values_to_be_between(
        "kg_payload", min_value=0, max_value=50000
    )
    validator.expect_column_values_to_be_between(
        "km_distancia", min_value=0, max_value=10000
    )
    return validator


def main():
    expectation_suite_name = "anac_expectation_suite"
    raw_path = "./data/raw/sample"
    asset_name = "my_asset"
    data_source_name = "my_pandas_datasource"

    context = gx.get_context()
    datasource = setup_datasource(
        context,
        data_source_name,
        raw_path,
        asset_name,
        r".*\.csv",
        schema=SCHEMA_SPARK,
    )
    print(datasource)

    data_asset = datasource.get_asset(asset_name)
    print(data_asset)
    batch_request = data_asset.build_batch_request()
    print(batch_request)

    context.add_or_update_expectation_suite(expectation_suite_name)
    print(context.list_expectation_suite_names())

    validator = setup_validator(context, batch_request, expectation_suite_name)
    validator.head()


if __name__ == "__main__":
    main()


# df.save_expectation_suite()

# # ----------------------------
# # 6. Rodar validação e gerar Data Docs
# # ----------------------------
# results = df.validate()
# df.build_data_docs()

# # Mostrar URL do relatório
# docs_url = df.get_docs_sites_urls()[0]["site_url"]
# print(f"Relatório de validação disponível em: {docs_url}")
