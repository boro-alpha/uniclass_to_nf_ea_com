from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_uuidifier import uuidify_dataframe
from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from pandas import DataFrame
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CHILD_UUID_COLUMN_NAME, \
    CHILD_CODE_COLUMN_NAME, CODE_COLUMN_NAME, CHILD_TITLE_COLUMN_NAME, TITLE_COLUMN_NAME, PARENT_CODE_COLUMN_NAME, \
    PARENT_UUID_COLUMN_NAME, PARENT_TITLE_COLUMN_NAME, UNICLASS2015_OBJECT_TABLE_NAME, \
    UNICLASS_PARENT_CHILD_LINK_TABLE_NAME, NF_UUIDS_COLUMN_NAME, UUID_COLUMN_NAME


def create_parent_child_link_table(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_2015_object_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    link_table_dataframe = \
        DataFrame()

    link_table_dataframe[
        CHILD_UUID_COLUMN_NAME] = \
        uniclass_2015_object_table[
            UUID_COLUMN_NAME]

    link_table_dataframe[
        CHILD_CODE_COLUMN_NAME] = \
        uniclass_2015_object_table[
            CODE_COLUMN_NAME]

    link_table_dataframe[
        CHILD_TITLE_COLUMN_NAME] = \
        uniclass_2015_object_table[
            TITLE_COLUMN_NAME]

    link_table_dataframe[
        PARENT_CODE_COLUMN_NAME] = \
        uniclass_2015_object_table[
            PARENT_CODE_COLUMN_NAME]

    parent_child_link_base_table = \
        __create_parent_child_link_table(
            link_table_dataframe=link_table_dataframe,
            uniclass_objects_dataframe=uniclass_2015_object_table)

    parent_child_link_table_clean = \
        __remove_rows_with_no_parent_code_values_from_dataframe(
            dataframe=parent_child_link_base_table)

    parent_child_link_table = \
        uuidify_dataframe(
            dataframe=parent_child_link_table_clean,
            uuid_column_name=NF_UUIDS_COLUMN_NAME)

    dictionary_of_dataframes[
        UNICLASS_PARENT_CHILD_LINK_TABLE_NAME] = \
        parent_child_link_table

    return \
        dictionary_of_dataframes


def __create_parent_child_link_table(
        link_table_dataframe: DataFrame,
        uniclass_objects_dataframe: DataFrame) \
        -> DataFrame:
    temporary_dataframe_column_names_renamed = {
        UUID_COLUMN_NAME: PARENT_UUID_COLUMN_NAME,
        TITLE_COLUMN_NAME: PARENT_TITLE_COLUMN_NAME
    }

    parent_child_link_table = \
        left_merge_dataframes(
            master_dataframe=link_table_dataframe,
            master_dataframe_key_columns=[PARENT_CODE_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=uniclass_objects_dataframe,
            foreign_key_dataframe_fk_columns=[CODE_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary=temporary_dataframe_column_names_renamed
        )

    parent_child_link_table = \
        parent_child_link_table[[
            CHILD_UUID_COLUMN_NAME,
            CHILD_CODE_COLUMN_NAME,
            CHILD_TITLE_COLUMN_NAME,
            PARENT_UUID_COLUMN_NAME,
            PARENT_CODE_COLUMN_NAME,
            PARENT_TITLE_COLUMN_NAME
        ]]

    return \
        parent_child_link_table


def __remove_rows_with_no_parent_code_values_from_dataframe(
        dataframe: DataFrame) \
        -> DataFrame:

    dataframe = \
        dataframe[~dataframe[PARENT_CODE_COLUMN_NAME].isnull()]

    return \
        dataframe

