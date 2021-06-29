import numpy as np

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import PARENT_CODE_COLUMN_NAME, \
    UNICLASS2015_OBJECT_TABLE_NAME


def add_parent_code_column_to_uniclass_objects_table(
        dictionary_of_dataframes: dict)\
        -> dict:
    uniclass_2015_object_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    uniclass_2015_object_table[
        PARENT_CODE_COLUMN_NAME] = \
        np.NaN

    dictionary_of_dataframes[
        UNICLASS2015_OBJECT_TABLE_NAME] = \
        uniclass_2015_object_table

    return \
        dictionary_of_dataframes
