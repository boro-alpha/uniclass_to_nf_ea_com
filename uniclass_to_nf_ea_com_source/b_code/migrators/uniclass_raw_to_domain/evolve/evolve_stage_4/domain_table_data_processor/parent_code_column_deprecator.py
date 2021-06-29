from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import PARENT_CODE_COLUMN_NAME, \
    UNICLASS2015_OBJECT_TABLE_NAME


def deprecate_parent_code_column_from_uniclass_object_table(
        dictionary_of_dataframes: dict) \
        -> dict:
    uniclass_objects_table = \
        dictionary_of_dataframes[
            UNICLASS2015_OBJECT_TABLE_NAME]

    uniclass_objects_table.drop(
        PARENT_CODE_COLUMN_NAME,
        axis=1,
        inplace=True)

    dictionary_of_dataframes[
        UNICLASS2015_OBJECT_TABLE_NAME] = \
        uniclass_objects_table

    return \
        dictionary_of_dataframes
