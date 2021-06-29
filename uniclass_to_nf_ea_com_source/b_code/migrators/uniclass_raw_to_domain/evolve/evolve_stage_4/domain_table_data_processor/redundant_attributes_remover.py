from numpy import NaN
from pandas import DataFrame
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CODE_COLUMN_NAME, \
    AREA_COLUMN_NAME, GROUP_COLUMN_NAME, SUB_GROUP_COLUMN_NAME, SECTION_COLUMN_NAME, UNICLASS2015_OBJECT_TABLE_NAME


def remove_redundant_attributes_from_object_table(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_object_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    for index in uniclass_object_table.index:
        __remove_redundant_attributes_from_object_table(
            uniclass_object_table=uniclass_object_table,
            index=index)

    dictionary_of_dataframes[
        UNICLASS2015_OBJECT_TABLE_NAME] = \
        uniclass_object_table

    return \
        dictionary_of_dataframes


def __remove_redundant_attributes_from_object_table(
        uniclass_object_table: DataFrame,
        index: int) \
        -> DataFrame:
    if len(uniclass_object_table.loc[index, CODE_COLUMN_NAME]) == 14:
        uniclass_object_table.loc[index, AREA_COLUMN_NAME] = NaN
        uniclass_object_table.loc[index, GROUP_COLUMN_NAME] = NaN
        uniclass_object_table.loc[index, SUB_GROUP_COLUMN_NAME] = NaN
        uniclass_object_table.loc[index, SECTION_COLUMN_NAME] = NaN

    if len(uniclass_object_table.loc[index, CODE_COLUMN_NAME]) == 11:
        uniclass_object_table.loc[index, AREA_COLUMN_NAME] = NaN
        uniclass_object_table.loc[index, GROUP_COLUMN_NAME] = NaN
        uniclass_object_table.loc[index, SUB_GROUP_COLUMN_NAME] = NaN

    if len(uniclass_object_table.loc[index, CODE_COLUMN_NAME]) == 8:
        uniclass_object_table.loc[index, AREA_COLUMN_NAME] = NaN
        uniclass_object_table.loc[index, GROUP_COLUMN_NAME] = NaN

    if len(uniclass_object_table.loc[index, CODE_COLUMN_NAME]) == 5:
        uniclass_object_table.loc[index, AREA_COLUMN_NAME] = NaN

    return \
        uniclass_object_table
