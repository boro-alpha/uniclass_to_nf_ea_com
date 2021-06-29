from pandas import DataFrame

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS2015_OBJECT_TABLE_NAME, CODE_COLUMN_NAME, UNICLASS_ITEM_NAME, RANKS_COLUMN_NAME, TOP_ITEM_NAME, AREA_NAME, \
    GROUP_NAME, SUB_GROUP_NAME, SECTION_NAME, OBJECT_NAME


def populate_uniclass_objects_table_ranks_column(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_2015_objects_table = \
        dictionary_of_dataframes[UNICLASS2015_OBJECT_TABLE_NAME]

    for index in uniclass_2015_objects_table.index:
        __populate_ranks_column_cell(
            uniclass_2015_objects_table=uniclass_2015_objects_table,
            index=index)

    return \
        dictionary_of_dataframes


def __populate_ranks_column_cell(
        uniclass_2015_objects_table: DataFrame,
        index: int):
    if uniclass_2015_objects_table.loc[index, CODE_COLUMN_NAME] == UNICLASS_ITEM_NAME:
        uniclass_2015_objects_table.loc[index, RANKS_COLUMN_NAME] = \
            TOP_ITEM_NAME
    if len(str(uniclass_2015_objects_table.loc[index, CODE_COLUMN_NAME])) == 2:
        uniclass_2015_objects_table.loc[index, RANKS_COLUMN_NAME] = \
            AREA_NAME
    if len(str(uniclass_2015_objects_table.loc[index, CODE_COLUMN_NAME])) == 5:
        uniclass_2015_objects_table.loc[index, RANKS_COLUMN_NAME] = \
            GROUP_NAME
    if len(str(uniclass_2015_objects_table.loc[index, CODE_COLUMN_NAME])) == 8:
        uniclass_2015_objects_table.loc[index, RANKS_COLUMN_NAME] = \
            SUB_GROUP_NAME
    if len(str(uniclass_2015_objects_table.loc[index, CODE_COLUMN_NAME])) == 11:
        uniclass_2015_objects_table.loc[index, RANKS_COLUMN_NAME] = \
            SECTION_NAME
    if len(str(uniclass_2015_objects_table.loc[index, CODE_COLUMN_NAME])) == 14:
        uniclass_2015_objects_table.loc[index, RANKS_COLUMN_NAME] = \
            OBJECT_NAME
