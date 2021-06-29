from pandas import DataFrame

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UNICLASS_STEREOTYPES_TYPE_OF_TABLE_NAME


def add_stereotypes_table_to_dataframes_dictionary(
        dictionary_of_dataframes: dict,
        stereotypes_table: DataFrame):

    dictionary_of_dataframes[
        UNICLASS_STEREOTYPES_TYPE_OF_TABLE_NAME] = \
        stereotypes_table

    return \
        dictionary_of_dataframes
