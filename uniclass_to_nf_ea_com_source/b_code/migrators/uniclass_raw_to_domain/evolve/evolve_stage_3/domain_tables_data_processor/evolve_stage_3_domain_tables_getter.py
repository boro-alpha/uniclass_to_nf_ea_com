from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_dictionary_uuidifier import \
    uuidify_dictionary_of_dataframes
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dictionary_of_dataframes_concatenator import \
    concatenate_dictionary_of_dataframes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS2015_OBJECT_TABLE_NAME
from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resources_namespace_constants import \
    EVOLVE_3_INPUT_FOLDER_NAMESPACE
from uniclass_to_nf_ea_com_source.b_code.services.input_output.all_csv_files_in_resource_namespace_into_dataframe_dictionary_loader import \
    load_all_csv_files_in_resource_namespace_into_dataframe_dictionary
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_3.domain_tables_data_processor.evolve_stage_3_domain_tables_parent_package_columns_populator import \
    populate_evolve_stage_3_domain_tables_parent_package_columns


def get_evolve_stage_3_domain_tables(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass2015_object_table = \
        concatenate_dictionary_of_dataframes(
            dataframe_dictionary=dictionary_of_dataframes)

    evolve_stage_3_input_tables = \
        load_all_csv_files_in_resource_namespace_into_dataframe_dictionary(
            resource_namespace=EVOLVE_3_INPUT_FOLDER_NAMESPACE)

    uuidified_evolve_stage_3_input_tables = \
        uuidify_dictionary_of_dataframes(
            dictionary_of_dataframes=evolve_stage_3_input_tables,
            uuid_column_name=StandardObjectTableColumnTypes.NF_UUIDS.column_name)

    evolve_stage_3_domain_tables = \
        populate_evolve_stage_3_domain_tables_parent_package_columns(
            dictionary_of_dataframes=uuidified_evolve_stage_3_input_tables)

    evolve_stage_3_domain_tables[UNICLASS2015_OBJECT_TABLE_NAME] = \
        uniclass2015_object_table

    return \
        evolve_stage_3_domain_tables
