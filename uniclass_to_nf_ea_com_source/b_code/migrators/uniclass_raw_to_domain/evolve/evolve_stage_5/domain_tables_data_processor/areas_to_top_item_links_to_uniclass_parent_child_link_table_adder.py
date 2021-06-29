from pandas import DataFrame, concat

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS_PARENT_CHILD_LINK_TABLE_NAME


def add_areas_to_top_item_links_to_uniclass_parent_child_link_table(
        dictionary_of_dataframes: dict,
        areas_to_top_item_temporary_link_table: DataFrame) \
        -> dict:
    uniclass_parent_child_link_table = \
        dictionary_of_dataframes[
            UNICLASS_PARENT_CHILD_LINK_TABLE_NAME]

    uniclass_parent_child_link_table_with_areas_linked_to_top_item = \
        concat([
            uniclass_parent_child_link_table,
            areas_to_top_item_temporary_link_table])

    dictionary_of_dataframes[
        UNICLASS_PARENT_CHILD_LINK_TABLE_NAME] = \
        uniclass_parent_child_link_table_with_areas_linked_to_top_item

    return \
        dictionary_of_dataframes
