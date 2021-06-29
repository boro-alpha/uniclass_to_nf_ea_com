from pandas import concat
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME
from uniclass_to_nf_ea_com_source.b_code.services.uniclass_common.uuidifed_packages_parent_ea_element_populator import \
    populate_uuidified_packages_parent_uuids_column


def add_evolve_7_packages_to_evolve_8_input_object_tables_and_uuidify_parents_column(
        evolve_stage_7_dictionary_of_dataframes: dict,
        uuidified_evolve_8_object_input_tables: dict) \
        -> dict:
    evolve_7_and_evolve_8_uuidified_packages_tables_union = \
        concat([
            evolve_stage_7_dictionary_of_dataframes[UUIDIFIED_PACKAGES_TABLE_NAME],
            uuidified_evolve_8_object_input_tables[UUIDIFIED_PACKAGES_TABLE_NAME]])

    uuidified_evolve_8_object_input_tables[UUIDIFIED_PACKAGES_TABLE_NAME] = \
        evolve_7_and_evolve_8_uuidified_packages_tables_union

    evolve_8_input_object_tables_with_packages_fully_uuidified = \
        populate_uuidified_packages_parent_uuids_column(
            dictionary_of_dataframes=uuidified_evolve_8_object_input_tables)

    return \
        evolve_8_input_object_tables_with_packages_fully_uuidified
