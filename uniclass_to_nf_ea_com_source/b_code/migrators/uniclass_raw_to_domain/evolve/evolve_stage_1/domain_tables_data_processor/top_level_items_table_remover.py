from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS2015_TOP_LEVEL_OBJECTS_TABLE_NAME


def remove_top_level_items_table_from_dataframes_dictionary(
        dataframe_dictionary: dict):
    top_level_dataframe_dictionary = {}

    for table, dataframe in dataframe_dictionary.items():
        if table != UNICLASS2015_TOP_LEVEL_OBJECTS_TABLE_NAME:
            top_level_dataframe_dictionary[table] = dataframe

    return \
        top_level_dataframe_dictionary
