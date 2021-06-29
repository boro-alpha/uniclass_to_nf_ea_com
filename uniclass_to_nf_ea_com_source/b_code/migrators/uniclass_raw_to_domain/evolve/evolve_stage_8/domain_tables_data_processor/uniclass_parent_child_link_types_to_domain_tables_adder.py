import pandas
from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS_PARENT_CHILD_LINK_TABLE_NAME, PARENT_CODE_COLUMN_NAME, PARENT_CHILD_SUBTYPES_TABLE_NAME, \
    RELATION_TYPE_NAMES_COLUMN_NAME, \
    RELATION_TYPE_UUIDS_COLUMN_NAME, UUID_COLUMN_NAME, PARENT_RANK_NAME_COLUMN_NAME, OBJECT_NAME_COLUMN_NAME, \
    UNICLASS2015_OBJECT_TABLE_NAME, CODE_COLUMN_NAME, RANKS_COLUMN_NAME


def add_uniclass_parent_child_link_types_to_domain_tables(
        evolve_8_domain_tables: dict) \
        -> dict:
    uniclass_parent_child_link_table = \
        evolve_8_domain_tables[
            UNICLASS_PARENT_CHILD_LINK_TABLE_NAME]

    uniclass_objects_table = \
        evolve_8_domain_tables[
            UNICLASS2015_OBJECT_TABLE_NAME]

    parent_child_subtypes_table = \
        evolve_8_domain_tables[
            PARENT_CHILD_SUBTYPES_TABLE_NAME]

    uniclass_parent_child_link_table_with_ranks = \
        __add_rank_column_to_uniclass_parent_child_link_table(
            uniclass_parent_child_link_table=uniclass_parent_child_link_table,
            uniclass_objects_table=uniclass_objects_table)

    uniclass_parent_child_link_table_with_connector_types = \
        __add_connector_types_to_uniclass_parent_child_link_table(
            uniclass_parent_child_link_table=uniclass_parent_child_link_table_with_ranks,
            parent_child_subtypes_table=parent_child_subtypes_table)

    evolve_8_domain_tables[
        UNICLASS_PARENT_CHILD_LINK_TABLE_NAME] = \
        uniclass_parent_child_link_table_with_connector_types

    return \
        evolve_8_domain_tables


def __add_rank_column_to_uniclass_parent_child_link_table(
        uniclass_parent_child_link_table: pandas.DataFrame,
        uniclass_objects_table: pandas.DataFrame) \
        -> pandas.DataFrame:
    uniclass_parent_child_link_table_with_parent_ranks = \
        left_merge_dataframes(
            master_dataframe=uniclass_parent_child_link_table,
            master_dataframe_key_columns=[PARENT_CODE_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=uniclass_objects_table,
            foreign_key_dataframe_fk_columns=[CODE_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary={
                RANKS_COLUMN_NAME: RANKS_COLUMN_NAME})

    return \
        uniclass_parent_child_link_table_with_parent_ranks


def __add_connector_types_to_uniclass_parent_child_link_table(
        uniclass_parent_child_link_table: pandas.DataFrame,
        parent_child_subtypes_table: pandas.DataFrame) \
        -> pandas.DataFrame:
    uniclass_parent_child_link_table_with_connector_types = \
        left_merge_dataframes(
            master_dataframe=uniclass_parent_child_link_table,
            master_dataframe_key_columns=[RANKS_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=parent_child_subtypes_table,
            foreign_key_dataframe_fk_columns=[PARENT_RANK_NAME_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary={
                UUID_COLUMN_NAME: RELATION_TYPE_UUIDS_COLUMN_NAME,
                OBJECT_NAME_COLUMN_NAME: RELATION_TYPE_NAMES_COLUMN_NAME})

    return \
        uniclass_parent_child_link_table_with_connector_types
