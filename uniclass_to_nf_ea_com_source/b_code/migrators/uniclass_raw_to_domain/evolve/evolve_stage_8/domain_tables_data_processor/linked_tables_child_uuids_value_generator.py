import pandas

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CHILD_PACKAGE_NAME_COLUMN_NAME, \
    PARENT_PACKAGE_NAME_COLUMN_NAME, UUIDIFIED_PACKAGES_TABLE_NAME, OBJECT_NAME_COLUMN_NAME, CHILD_NAMES_COLUMN_NAME, \
    CHILD_UUIDS_COLUMN_NAME, UUID_COLUMN_NAME


def __add_child_uuids_value(
        dataframe_linked_table: pandas.DataFrame,
        evolve_8_object_tables: dict) \
        -> pandas.DataFrame:
    for table_name_object_tables, dataframe_object_tables in evolve_8_object_tables.items():
        if table_name_object_tables != UUIDIFIED_PACKAGES_TABLE_NAME:
            if dataframe_linked_table.loc[
                CHILD_PACKAGE_NAME_COLUMN_NAME] == \
                    dataframe_object_tables.loc[
                        PARENT_PACKAGE_NAME_COLUMN_NAME] and \
                    dataframe_linked_table.loc[
                        CHILD_NAMES_COLUMN_NAME] == \
                    dataframe_object_tables.loc[
                        OBJECT_NAME_COLUMN_NAME]:
                dataframe_linked_table.loc[
                    CHILD_UUIDS_COLUMN_NAME] = \
                    dataframe_object_tables.loc[
                        UUID_COLUMN_NAME]

    return \
        dataframe_linked_table
