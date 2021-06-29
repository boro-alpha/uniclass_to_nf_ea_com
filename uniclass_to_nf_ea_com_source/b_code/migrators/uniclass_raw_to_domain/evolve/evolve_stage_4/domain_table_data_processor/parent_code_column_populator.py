import numpy as np
from pandas import DataFrame

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CODE_COLUMN_NAME, \
    PARENT_CODE_COLUMN_NAME, UNICLASS2015_OBJECT_TABLE_NAME


def populate_parent_code_column_in_uniclass_object_table(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_2015_object_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    for index in uniclass_2015_object_table.index:
        __add_value_to_parent_code_column(
            dataframe=uniclass_2015_object_table,
            index=index)

    dictionary_of_dataframes[
        UNICLASS2015_OBJECT_TABLE_NAME] = \
        uniclass_2015_object_table

    return \
        dictionary_of_dataframes


def __add_value_to_parent_code_column(
        dataframe: DataFrame,
        index: int) \
        -> DataFrame:
    if len(dataframe.loc[index, CODE_COLUMN_NAME]) == 2:
        dataframe.loc[index, PARENT_CODE_COLUMN_NAME] = \
            np.NaN

    elif len(dataframe.loc[index, CODE_COLUMN_NAME]) > 2:
        dataframe.loc[index, PARENT_CODE_COLUMN_NAME] = \
            dataframe.loc[index, CODE_COLUMN_NAME][:-3]

    return \
        dataframe
