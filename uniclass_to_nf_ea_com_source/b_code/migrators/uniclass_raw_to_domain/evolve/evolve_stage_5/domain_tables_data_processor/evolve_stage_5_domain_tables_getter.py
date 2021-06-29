from nf_common_source.code.services.identification_services.uuid_service.uuid_helpers.uuid_factory import \
    create_new_uuid
from pandas import DataFrame

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUID_COLUMN_NAME, \
    CODE_COLUMN_NAME, TITLE_COLUMN_NAME, UNICLASS_ITEM_NAME, UNICLASS2015_OBJECT_TABLE_NAME, NF_UUIDS_COLUMN_NAME, \
    CHILD_UUID_COLUMN_NAME, CHILD_CODE_COLUMN_NAME, CHILD_TITLE_COLUMN_NAME, PARENT_UUID_COLUMN_NAME, \
    PARENT_CODE_COLUMN_NAME, PARENT_TITLE_COLUMN_NAME
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_5.domain_tables_data_processor.areas_to_top_item_links_to_uniclass_parent_child_link_table_adder import add_areas_to_top_item_links_to_uniclass_parent_child_link_table


def get_evolve_stage_5_domain_tables(
        dictionary_of_dataframes: dict) \
        -> dict:
    evolve_stage_5_domain_tables_with_top_item_added_to_objects_table = \
        __add_top_element_row_to_uniclass_objects_table(
            dictionary_of_dataframes=dictionary_of_dataframes)

    areas_to_top_item_temporary_link_table = \
        __create_areas_to_top_item_temporary_link_table(
            dictionary_of_dataframes=dictionary_of_dataframes)

    evolve_stage_5_domain_tables = \
        add_areas_to_top_item_links_to_uniclass_parent_child_link_table(
            dictionary_of_dataframes=evolve_stage_5_domain_tables_with_top_item_added_to_objects_table,
            areas_to_top_item_temporary_link_table=areas_to_top_item_temporary_link_table)

    return \
        evolve_stage_5_domain_tables


def __add_top_element_row_to_uniclass_objects_table(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_item_uuid = \
        create_new_uuid()

    object_table_top_element_row = \
        {
            UUID_COLUMN_NAME: uniclass_item_uuid,
            CODE_COLUMN_NAME: UNICLASS_ITEM_NAME,
            TITLE_COLUMN_NAME: UNICLASS_ITEM_NAME
        }

    dictionary_of_dataframes[UNICLASS2015_OBJECT_TABLE_NAME] = \
        dictionary_of_dataframes[UNICLASS2015_OBJECT_TABLE_NAME].append(
            object_table_top_element_row,
            ignore_index=True)

    return \
        dictionary_of_dataframes


def __create_areas_to_top_item_temporary_link_table(
        dictionary_of_dataframes: dict) \
        -> DataFrame:
    object_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    uniclass_item_uuid = \
        object_table.loc[
            object_table[
                TITLE_COLUMN_NAME] == UNICLASS_ITEM_NAME,
            UUID_COLUMN_NAME].to_string(index=False).strip()

    areas_to_top_item_temporary_link_table = \
        DataFrame()

    for index in object_table.index:
        if len(object_table.loc[index, CODE_COLUMN_NAME]) == 2:
            areas_to_top_item_temporary_link_table.loc[index, NF_UUIDS_COLUMN_NAME] = \
                create_new_uuid()
            areas_to_top_item_temporary_link_table.loc[index, CHILD_UUID_COLUMN_NAME] = \
                object_table.loc[index, UUID_COLUMN_NAME]
            areas_to_top_item_temporary_link_table.loc[index, CHILD_CODE_COLUMN_NAME] = \
                object_table.loc[index, CODE_COLUMN_NAME]
            areas_to_top_item_temporary_link_table.loc[index, CHILD_TITLE_COLUMN_NAME] = \
                object_table.loc[index, TITLE_COLUMN_NAME]
            areas_to_top_item_temporary_link_table.loc[index, PARENT_UUID_COLUMN_NAME] = \
                uniclass_item_uuid
            areas_to_top_item_temporary_link_table.loc[index, PARENT_CODE_COLUMN_NAME] = \
                UNICLASS_ITEM_NAME
            areas_to_top_item_temporary_link_table.loc[index, PARENT_TITLE_COLUMN_NAME] = \
                UNICLASS_ITEM_NAME

    return \
        areas_to_top_item_temporary_link_table
