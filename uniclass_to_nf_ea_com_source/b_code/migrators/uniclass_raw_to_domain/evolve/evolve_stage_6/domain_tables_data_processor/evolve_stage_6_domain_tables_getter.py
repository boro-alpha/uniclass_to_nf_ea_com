from nf_common_source.code.services.resources_service.processes.resource_file_getter import get_resource_file
from numpy import NaN

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS2015_OBJECT_TABLE_NAME, RANKS_COLUMN_NAME, UNICLASS2015_RANKS_TABLE_NAME, UUIDIFIED_PACKAGES_TABLE_NAME
from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resource_file_constants import \
    EVOLVE_6_UNICLASS_RANKS_FILE_NAME
from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resources_namespace_constants import \
    EVOLVE_6_INPUT_FOLDER_NAMESPACE
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_6.domain_tables_data_processor.uniclass_items_to_ranks_table_creator import \
    create_uniclass_items_to_ranks_table_and_add_to_dictionary
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_6.domain_tables_data_processor.ranks_column_populator import \
    populate_uniclass_objects_table_ranks_column
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_6.domain_tables_data_processor.uuidified_ranks_table_generator import \
    get_uuidified_ranks_table
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_6.domain_tables_data_processor.uniclass_ranks_package_row_to_uuidified_packages_adder import add_uniclass_ranks_package_row_to_uuidified_packages
from uniclass_to_nf_ea_com_source.b_code.services.uniclass_common.dataframe_parent_package_column_populator import \
    populate_parent_package_uuid_column_in_dataframe


def get_evolve_stage_6_domain_tables(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_2015_ranks_table_file = \
        get_resource_file(
            resource_namespace=EVOLVE_6_INPUT_FOLDER_NAMESPACE,
            resource_name=EVOLVE_6_UNICLASS_RANKS_FILE_NAME)

    dictionary_of_dataframes[UNICLASS2015_OBJECT_TABLE_NAME][RANKS_COLUMN_NAME] = \
        NaN

    evolve_stage_6_dictionary_with_uniclass_objects_table_ranks_column_populated = \
        populate_uniclass_objects_table_ranks_column(
            dictionary_of_dataframes=dictionary_of_dataframes)

    uuidified_ranks_table = \
        get_uuidified_ranks_table(
            csv_file_path=uniclass_2015_ranks_table_file.absolute_path_string)

    evolve_stage_6_dictionary_with_uniclass_ranks_added_to_packages = \
        add_uniclass_ranks_package_row_to_uuidified_packages(
            dictionary_of_dataframes=evolve_stage_6_dictionary_with_uniclass_objects_table_ranks_column_populated)

    uuidified_ranks_table = \
        populate_parent_package_uuid_column_in_dataframe(
            dataframe_name=UNICLASS2015_RANKS_TABLE_NAME,
            dataframe=uuidified_ranks_table,
            uuidified_packages_dataframe=evolve_stage_6_dictionary_with_uniclass_ranks_added_to_packages[
                UUIDIFIED_PACKAGES_TABLE_NAME])

    evolve_stage_6_dictionary_with_uniclass_ranks_added_to_packages[UNICLASS2015_RANKS_TABLE_NAME] = \
        uuidified_ranks_table

    evolve_stage_6_domain_tables = \
        create_uniclass_items_to_ranks_table_and_add_to_dictionary(
            dictionary_of_dataframes=evolve_stage_6_dictionary_with_uniclass_ranks_added_to_packages)

    return \
        evolve_stage_6_domain_tables
