from pandas import DataFrame
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import TOP_ITEMS_TABLE_NAME, \
    CODE_COLUMN_NAME, TITLE_COLUMN_NAME, UUID_COLUMN_NAME


def add_top_level_item_rows_to_dictionary_of_dataframes(
        dictionary_of_dataframes: dict) \
        -> dict:
    top_level_dataframe_dictionary = \
        {}

    uniclass_top_item_dataframe = \
        dictionary_of_dataframes[
            TOP_ITEMS_TABLE_NAME]

    for index, row in uniclass_top_item_dataframe.iterrows():
        top_level_dataframe_dictionary = \
            __add_top_item_row_to_dataframe(
                dictionary_of_dataframes=dictionary_of_dataframes,
                row=row)

    return \
        top_level_dataframe_dictionary


def __add_top_item_row_to_dataframe(
        dictionary_of_dataframes: dict,
        row) \
        -> dict:
    for table_name, dataframe in dictionary_of_dataframes.items():
        dataframe_with_top_item = \
            __generate_top_level_row_dataframe(
                row=row,
                dataframe=dataframe)

        dictionary_of_dataframes[table_name] = \
            dataframe_with_top_item

    return \
        dictionary_of_dataframes


def __generate_top_level_row_dataframe(
        row,
        dataframe) \
        -> DataFrame:
    top_level_row_dictionary = \
        {}

    if row[CODE_COLUMN_NAME] in dataframe[CODE_COLUMN_NAME][0]:
        top_level_row_dictionary[UUID_COLUMN_NAME] = \
            row[UUID_COLUMN_NAME]
        top_level_row_dictionary[CODE_COLUMN_NAME] = \
            row[CODE_COLUMN_NAME]
        top_level_row_dictionary[TITLE_COLUMN_NAME] = \
            row[TITLE_COLUMN_NAME]

        dataframe = dataframe.append(
            top_level_row_dictionary,
            ignore_index=True)

    return \
        dataframe
