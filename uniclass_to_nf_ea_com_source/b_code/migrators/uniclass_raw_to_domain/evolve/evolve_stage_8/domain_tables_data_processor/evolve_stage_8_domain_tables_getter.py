from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_dictionary_uuidifier import uuidify_dictionary_of_dataframes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import StandardObjectTableColumnTypes
from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resources_namespace_constants import EVOLVE_8_OBJECT_TABLES_INPUT_FOLDER_NAMESPACE
from uniclass_to_nf_ea_com_source.b_code.configurations.resource_constants.resources_namespace_constants import EVOLVE_8_LINKED_TABLES_INPUT_FOLDER_NAMESPACE
from uniclass_to_nf_ea_com_source.b_code.services.input_output.all_csv_files_in_resource_namespace_into_dataframe_dictionary_loader import load_all_csv_files_in_resource_namespace_into_dataframe_dictionary
from uniclass_to_nf_ea_com_source.b_code.services.uniclass_common.dataframes_dictionary_parent_package_column_populator import populate_dataframes_dictionary_parent_package_columns
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.uniclass_items_to_ranks_link_types_to_domain_tables_adder import add_uniclass_items_to_ranks_link_types_to_domain_tables
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.evolve_7_tables_to_evolve_8_input_object_tables_adder import add_evolve_7_tables_to_evolve_8_input_object_tables
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.evolve_7_packages_adder_and_packages_parents_uuidifier import add_evolve_7_packages_to_evolve_8_input_object_tables_and_uuidify_parents_column
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.child_parent_uuids_in_evolve_8_input_linked_tables_populator import populate_child_parent_uuids_in_evolve_8_input_linked_tables
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.uniclass_parent_child_link_types_to_domain_tables_adder import add_uniclass_parent_child_link_types_to_domain_tables
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.uniclass_objects_table_columns_standariser import standardise_uniclass_objects_table_columns
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_8.domain_tables_data_processor.evolve_stage_8_domain_tables_updater import update_evolve_stage_8_domain_tables_with_input_linked_tables


def get_evolve_stage_8_domain_tables(
        evolve_stage_7_dictionary_of_dataframes: dict) \
        -> dict:
    evolve_stage_7_dictionary_with_uniclass_objects_table_standardised = \
        standardise_uniclass_objects_table_columns(
            evolve_stage_7_dictionary_of_dataframes=evolve_stage_7_dictionary_of_dataframes)

    evolve_8_object_input_tables = \
        load_all_csv_files_in_resource_namespace_into_dataframe_dictionary(
            resource_namespace=EVOLVE_8_OBJECT_TABLES_INPUT_FOLDER_NAMESPACE)

    evolve_8_linked_input_tables = \
        load_all_csv_files_in_resource_namespace_into_dataframe_dictionary(
            resource_namespace=EVOLVE_8_LINKED_TABLES_INPUT_FOLDER_NAMESPACE)

    uuidified_evolve_8_linked_input_tables = \
        uuidify_dictionary_of_dataframes(
            dictionary_of_dataframes=evolve_8_linked_input_tables,
            uuid_column_name=StandardObjectTableColumnTypes.NF_UUIDS.column_name)

    uuidified_evolve_8_object_input_tables = \
        uuidify_dictionary_of_dataframes(
            dictionary_of_dataframes=evolve_8_object_input_tables,
            uuid_column_name=StandardObjectTableColumnTypes.NF_UUIDS.column_name)

    evolve_8_input_object_tables_with_packages_fully_uuidified = \
        add_evolve_7_packages_to_evolve_8_input_object_tables_and_uuidify_parents_column(
            evolve_stage_7_dictionary_of_dataframes=evolve_stage_7_dictionary_with_uniclass_objects_table_standardised,
            uuidified_evolve_8_object_input_tables=uuidified_evolve_8_object_input_tables)

    evolve_8_input_object_tables_parent_package_column_populated = \
        populate_dataframes_dictionary_parent_package_columns(
            dictionary_of_dataframes=evolve_8_input_object_tables_with_packages_fully_uuidified)

    evolve_8_input_object_tables_plus_evolve_7_domain_tables = \
        add_evolve_7_tables_to_evolve_8_input_object_tables(
            evolve_stage_7_dictionary_of_dataframes=evolve_stage_7_dictionary_with_uniclass_objects_table_standardised,
            evolve_8_object_tables_parent_package_column_populated=
            evolve_8_input_object_tables_parent_package_column_populated)

    evolve_8_domain_tables_with_uniclass_parent_child_link_types = \
        add_uniclass_parent_child_link_types_to_domain_tables(
            evolve_8_domain_tables=evolve_8_input_object_tables_plus_evolve_7_domain_tables)

    evolve_8_domain_tables_with_uniclass_items_to_ranks_link_types = \
        add_uniclass_items_to_ranks_link_types_to_domain_tables(
            evolve_8_domain_tables=evolve_8_domain_tables_with_uniclass_parent_child_link_types)

    evolve_8_linked_tables_with_link_uuid_populated = \
        populate_child_parent_uuids_in_evolve_8_input_linked_tables(
            evolve_8_input_linked_tables=uuidified_evolve_8_linked_input_tables,
            evolve_8_domain_tables=evolve_8_domain_tables_with_uniclass_items_to_ranks_link_types)

    evolve_stage_8_domain_tables = \
        update_evolve_stage_8_domain_tables_with_input_linked_tables(
            evolve_8_domain_tables=evolve_8_domain_tables_with_uniclass_items_to_ranks_link_types,
            evolve_8_input_linked_tables=evolve_8_linked_tables_with_link_uuid_populated)

    return \
        evolve_stage_8_domain_tables
