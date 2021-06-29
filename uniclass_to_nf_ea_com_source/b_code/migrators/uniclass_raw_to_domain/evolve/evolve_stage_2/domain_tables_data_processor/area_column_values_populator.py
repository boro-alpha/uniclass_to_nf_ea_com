from pandas import DataFrame
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CODE_COLUMN_NAME, \
    AREA_COLUMN_NAME


def populate_area_column(
        dictionary_of_dataframes: dict) \
        -> dict:
    for dataframe in dictionary_of_dataframes.values():
        __value_to_area_column(
                dataframe=dataframe)

    return \
        dictionary_of_dataframes


def __value_to_area_column(
        dataframe: DataFrame) \
        -> DataFrame:
    for index in dataframe.index:
        dataframe.loc[index, AREA_COLUMN_NAME] = \
            dataframe.loc[index, CODE_COLUMN_NAME][:2]

    return \
        dataframe
