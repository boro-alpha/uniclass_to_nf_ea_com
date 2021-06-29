from nf_common_source.code.services.resources_service.processes.resource_file_getter import get_resource_file
from pandas import read_csv

from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resource_file_constants import \
    EVOLVE_7_UNICLASS_RANKS_PARENT_CHILD_FILE_NAME
from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resources_namespace_constants import \
    EVOLVE_7_INPUT_FOLDER_NAMESPACE
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_7.domain_tables_data_processor.uniclass_ranks_table_in_domain_tables_dictionary_updater import \
    update_uniclass_ranks_table_in_domain_tables_dictionary
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_7.domain_tables_data_processor.child_uuids_and_names_to_uniclass_ranks_table_adder import \
    add_child_uuids_and_names_to_uniclass_ranks_table


def get_evolve_stage_7_domain_tables(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_ranks_parent_child_file = \
        get_resource_file(
            resource_namespace=EVOLVE_7_INPUT_FOLDER_NAMESPACE,
            resource_name=EVOLVE_7_UNICLASS_RANKS_PARENT_CHILD_FILE_NAME)

    uniclass_ranks_parent_child_table = \
        read_csv(
            uniclass_ranks_parent_child_file.absolute_path_string)

    uniclass_ranks_table_with_child_uuids_and_names = \
        add_child_uuids_and_names_to_uniclass_ranks_table(
            dictionary_of_dataframes=dictionary_of_dataframes,
            dataframe=uniclass_ranks_parent_child_table)

    evolve_stage_7_domain_tables = \
        update_uniclass_ranks_table_in_domain_tables_dictionary(
            dictionary_of_dataframes=dictionary_of_dataframes,
            dataframe=uniclass_ranks_table_with_child_uuids_and_names)

    return \
        evolve_stage_7_domain_tables
