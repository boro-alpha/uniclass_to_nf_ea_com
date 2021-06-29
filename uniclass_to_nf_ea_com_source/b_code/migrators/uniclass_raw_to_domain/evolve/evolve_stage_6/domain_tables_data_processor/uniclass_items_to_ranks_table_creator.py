from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_uuidifier import uuidify_dataframe
from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_connector_table_column_types import \
    StandardConnectorTableColumnTypes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes
from numpy import NaN
from pandas import DataFrame

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UNICLASS2015_OBJECT_TABLE_NAME, \
    CHILD_UUID_COLUMN_NAME, NF_UUIDS_COLUMN_NAME, UUID_COLUMN_NAME, LINK_TYPES_COLUMN_NAME, DEPENDENCY_NAME, \
    UNICLASS2015_RANKS_TABLE_NAME, RANKS_COLUMN_NAME, PARENT_UUIDS_COLUMN_NAME, \
    LINKED_TABLE_UNICLASS_ITEMS_TO_RANKS, LINK_NAMES_COLUMN_NAME


def create_uniclass_items_to_ranks_table_and_add_to_dictionary(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_items_to_ranks_table = \
        __get_uniclass_items_to_ranks_table(
            dictionary_of_dataframes=dictionary_of_dataframes)

    uuidified_uniclass_items_to_ranks_table = \
        uuidify_dataframe(
            dataframe=uniclass_items_to_ranks_table,
            uuid_column_name=NF_UUIDS_COLUMN_NAME)

    dictionary_of_dataframes[
        LINKED_TABLE_UNICLASS_ITEMS_TO_RANKS] = \
        uuidified_uniclass_items_to_ranks_table

    return \
        dictionary_of_dataframes


def __get_uniclass_items_to_ranks_table(
        dictionary_of_dataframes: dict) \
        -> DataFrame:
    uniclass_items_to_ranks_table = \
        DataFrame()

    uniclass_objects_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    ranks_table = \
        dictionary_of_dataframes[UNICLASS2015_RANKS_TABLE_NAME]

    for index in uniclass_objects_table.index:
        uniclass_items_to_ranks_table.loc[index, CHILD_UUID_COLUMN_NAME] = \
            uniclass_objects_table.loc[index, UUID_COLUMN_NAME]
        uniclass_items_to_ranks_table.loc[index, RANKS_COLUMN_NAME] = \
            uniclass_objects_table.loc[index, RANKS_COLUMN_NAME]
        uniclass_items_to_ranks_table.loc[index, LINK_TYPES_COLUMN_NAME] = \
            DEPENDENCY_NAME
        uniclass_items_to_ranks_table.loc[index, LINK_NAMES_COLUMN_NAME] = \
            NaN

    populated_uniclass_items_to_ranks_table = \
        left_merge_dataframes(
            master_dataframe=uniclass_items_to_ranks_table,
            master_dataframe_key_columns=[RANKS_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=ranks_table,
            foreign_key_dataframe_fk_columns=[StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name],
            foreign_key_dataframe_other_column_rename_dictionary={
                UUID_COLUMN_NAME: PARENT_UUIDS_COLUMN_NAME})

    populated_uniclass_items_to_ranks_table = \
        dataframe_filter_and_rename(
            dataframe=populated_uniclass_items_to_ranks_table,
            filter_and_rename_dictionary={
                CHILD_UUID_COLUMN_NAME: StandardConnectorTableColumnTypes.SUPPLIER_PLACE_1_NF_UUIDS.column_name,
                PARENT_UUIDS_COLUMN_NAME: StandardConnectorTableColumnTypes.CLIENT_PLACE_2_NF_UUIDS.column_name,
                LINK_TYPES_COLUMN_NAME: StandardConnectorTableColumnTypes.CONNECTOR_UML_TYPE_IDENTIFIERS.column_name,
                LINK_NAMES_COLUMN_NAME: StandardConnectorTableColumnTypes.CONNECTOR_UML_NAMES.column_name})

    return \
        populated_uniclass_items_to_ranks_table
