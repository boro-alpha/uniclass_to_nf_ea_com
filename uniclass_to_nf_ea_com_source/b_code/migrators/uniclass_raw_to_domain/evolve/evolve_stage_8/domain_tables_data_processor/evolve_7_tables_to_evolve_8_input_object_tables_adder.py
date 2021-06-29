from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME


def add_evolve_7_tables_to_evolve_8_input_object_tables(
        evolve_stage_7_dictionary_of_dataframes: dict,
        evolve_8_object_tables_parent_package_column_populated: dict) \
        -> dict:
    evolve_stage_7_dictionary_of_dataframes.pop(
        UUIDIFIED_PACKAGES_TABLE_NAME)

    evolve_8_object_tables_parent_package_column_populated.update(
        evolve_stage_7_dictionary_of_dataframes)

    return \
        evolve_8_object_tables_parent_package_column_populated
