from uniclass_to_nf_ea_com_source.b_code.services.uniclass_common.dataframes_dictionary_parent_package_column_populator import \
    populate_dataframes_dictionary_parent_package_columns
from uniclass_to_nf_ea_com_source.b_code.services.uniclass_common.uuidifed_packages_parent_ea_element_populator import \
    populate_uuidified_packages_parent_uuids_column


def populate_evolve_stage_3_domain_tables_parent_package_columns(
        dictionary_of_dataframes: dict) \
        -> dict:
    dictionary_of_dataframes_with_uuidified_packages_parent_uuids_column_populated = \
        populate_uuidified_packages_parent_uuids_column(
            dictionary_of_dataframes=dictionary_of_dataframes)

    dictionary_of_dataframes_with_all_parent_package_columns_populated = \
        populate_dataframes_dictionary_parent_package_columns(
            dictionary_of_dataframes=dictionary_of_dataframes_with_uuidified_packages_parent_uuids_column_populated)

    return \
        dictionary_of_dataframes_with_all_parent_package_columns_populated
