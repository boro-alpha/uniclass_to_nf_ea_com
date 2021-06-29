from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME, \
    NF_UUIDS_COLUMN_NAME, PARENT_EA_ELEMENT1_COLUMN_NAME, PARENT_EA_ELEMENT_COLUMN_NAME, EA_GUIDS_COLUMN_NAME, \
    EA_PACKAGE_PATH_COLUMN_NAME, PARENT_PACKAGE_EA_GUID_COLUMN_NAME, CONTAINED_EA_PACKAGES_COLUMN_NAME, \
    EA_OBJECT_TYPE_COLUMN_NAME, SUPPLIER_PLACE1_END_CONNECTORS_COLUMN_NAME, \
    CLIENT_PLACE2_END_CONNECTORS_COLUMN_NAME, CONTAINED_EA_DIAGRAMS_COLUMN_NAME, CONTAINED_EA_CLASSIFIERS_COLUMN_NAME, \
    EA_OBJECT_STEREOTYPES_COLUMN_NAME, EA_REPOSITORY_COLUMN_NAME, EA_OBJECT_NAME_COLUMN_NAME, \
    EA_OBJECT_NOTES_COLUMN_NAME, EA_GUID_COLUMN_NAME, PARENT_EA_ELEMENT_NAME_COLUMN_NAME, \
    PARENT_PACKAGE_NAME_COLUMN_NAME


def populate_uuidified_packages_parent_uuids_column(
        dictionary_of_dataframes: dict) \
        -> dict:
    uuidified_packages_dataframe = \
        dictionary_of_dataframes[UUIDIFIED_PACKAGES_TABLE_NAME].copy()

    uuidified_packages_dataframe_renamed_column = \
        {
            NF_UUIDS_COLUMN_NAME: PARENT_EA_ELEMENT1_COLUMN_NAME
        }

    uuidified_packages_dataframe_with_ea_element_column_populated = \
        left_merge_dataframes(
            master_dataframe=uuidified_packages_dataframe,
            master_dataframe_key_columns=[PARENT_EA_ELEMENT_NAME_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=dictionary_of_dataframes[UUIDIFIED_PACKAGES_TABLE_NAME],
            foreign_key_dataframe_fk_columns=[EA_OBJECT_NAME_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary=uuidified_packages_dataframe_renamed_column)

    uuidified_packages_dataframe_with_ea_element_column_populated[PARENT_EA_ELEMENT_COLUMN_NAME].fillna(
        uuidified_packages_dataframe_with_ea_element_column_populated[PARENT_EA_ELEMENT1_COLUMN_NAME],
        inplace=True)

    uuidified_packages_dataframe_columns_reorder = \
        uuidified_packages_dataframe_with_ea_element_column_populated[[
            EA_GUIDS_COLUMN_NAME,
            EA_PACKAGE_PATH_COLUMN_NAME,
            PARENT_PACKAGE_EA_GUID_COLUMN_NAME,
            PARENT_PACKAGE_NAME_COLUMN_NAME,
            NF_UUIDS_COLUMN_NAME,
            CONTAINED_EA_PACKAGES_COLUMN_NAME,
            EA_OBJECT_TYPE_COLUMN_NAME,
            SUPPLIER_PLACE1_END_CONNECTORS_COLUMN_NAME,
            CLIENT_PLACE2_END_CONNECTORS_COLUMN_NAME,
            CONTAINED_EA_DIAGRAMS_COLUMN_NAME,
            CONTAINED_EA_CLASSIFIERS_COLUMN_NAME,
            PARENT_EA_ELEMENT_COLUMN_NAME,
            EA_OBJECT_STEREOTYPES_COLUMN_NAME,
            EA_REPOSITORY_COLUMN_NAME,
            EA_OBJECT_NAME_COLUMN_NAME,
            EA_OBJECT_NOTES_COLUMN_NAME,
            EA_GUID_COLUMN_NAME]]

    dictionary_of_dataframes[UUIDIFIED_PACKAGES_TABLE_NAME] = \
        uuidified_packages_dataframe_columns_reorder

    return \
        dictionary_of_dataframes
