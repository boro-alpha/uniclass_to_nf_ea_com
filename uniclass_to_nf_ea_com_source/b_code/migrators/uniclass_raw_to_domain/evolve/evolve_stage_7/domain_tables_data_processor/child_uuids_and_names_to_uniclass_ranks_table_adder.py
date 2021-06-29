from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes
from pandas import DataFrame

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS2015_RANKS_TABLE_NAME, PARENT_RANK_NAME_COLUMN_NAME, CHILD_RANK_NAME_COLUMN_NAME, \
    CHILD_UUID_COLUMN_NAME


def add_child_uuids_and_names_to_uniclass_ranks_table(
        dictionary_of_dataframes: dict,
        dataframe: DataFrame) \
        -> DataFrame:
    uniclass_ranks_table = \
        dictionary_of_dataframes[UNICLASS2015_RANKS_TABLE_NAME].copy()

    uniclass_ranks_parent_child_table = \
        dataframe.copy()

    uniclass_ranks_table_with_child_names = \
        left_merge_dataframes(
            master_dataframe=uniclass_ranks_table,
            master_dataframe_key_columns=[StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=uniclass_ranks_parent_child_table,
            foreign_key_dataframe_fk_columns=[PARENT_RANK_NAME_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary={
                CHILD_RANK_NAME_COLUMN_NAME: CHILD_RANK_NAME_COLUMN_NAME
            })

    uniclass_ranks_table_with_child_names_and_child_uuids = \
        left_merge_dataframes(
            master_dataframe=uniclass_ranks_table_with_child_names,
            master_dataframe_key_columns=[CHILD_RANK_NAME_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=uniclass_ranks_table,
            foreign_key_dataframe_fk_columns=[StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name],
            foreign_key_dataframe_other_column_rename_dictionary={
                StandardObjectTableColumnTypes.NF_UUIDS.column_name: CHILD_UUID_COLUMN_NAME
            })

    return \
        uniclass_ranks_table_with_child_names_and_child_uuids
