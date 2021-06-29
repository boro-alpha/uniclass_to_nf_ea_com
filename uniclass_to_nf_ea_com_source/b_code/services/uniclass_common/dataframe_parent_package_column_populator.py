from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from pandas import DataFrame
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME, \
    NF_UUIDS_COLUMN_NAME, PARENT_PACKAGE_UUID_COLUMN_NAME, PARENT_PACKAGE_NAME_COLUMN_NAME, EA_OBJECT_NAME_COLUMN_NAME, \
    PARENT_PACKAGE_UUID2_COLUMN_NAME, PARENT_PACKAGE_UUID1_COLUMN_NAME


def populate_parent_package_uuid_column_in_dataframe(
        dataframe_name: str,
        dataframe: DataFrame,
        uuidified_packages_dataframe: DataFrame) \
        -> DataFrame:
    if dataframe_name == UUIDIFIED_PACKAGES_TABLE_NAME:
        return \
            dataframe

    dataframe_with_parent_package_column_populated = \
        left_merge_dataframes(
            master_dataframe=dataframe,
            master_dataframe_key_columns=[PARENT_PACKAGE_NAME_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=uuidified_packages_dataframe,
            foreign_key_dataframe_fk_columns=[EA_OBJECT_NAME_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary={
                NF_UUIDS_COLUMN_NAME: PARENT_PACKAGE_UUID1_COLUMN_NAME
            })

    dataframe_with_parent_package_column_populated_columns_renamed = \
        dataframe_with_parent_package_column_populated.rename(
            columns={PARENT_PACKAGE_UUID_COLUMN_NAME: PARENT_PACKAGE_UUID2_COLUMN_NAME,
                     PARENT_PACKAGE_UUID1_COLUMN_NAME: PARENT_PACKAGE_UUID_COLUMN_NAME})

    dataframe_with_parent_package_column_populated_columns_renamed.drop(
        [PARENT_PACKAGE_UUID2_COLUMN_NAME],
        axis=1,
        inplace=True)

    return \
        dataframe_with_parent_package_column_populated_columns_renamed
