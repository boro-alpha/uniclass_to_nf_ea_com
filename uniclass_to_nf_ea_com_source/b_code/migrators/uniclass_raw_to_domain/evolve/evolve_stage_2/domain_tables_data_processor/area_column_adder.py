from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import AREA_NAME


def add_area_column(
        dictionary_of_dataframes: dict) \
        -> dict:
    for table, dataframe in dictionary_of_dataframes.items():
        dataframe.insert(
            2,
            AREA_NAME,
            '',
            allow_duplicates=True)

    return \
        dictionary_of_dataframes
