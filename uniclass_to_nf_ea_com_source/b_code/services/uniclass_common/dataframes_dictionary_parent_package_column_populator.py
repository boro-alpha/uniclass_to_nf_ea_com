from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME
from uniclass_to_nf_ea_com_source.b_code.services.uniclass_common.dataframe_parent_package_column_populator import \
    populate_parent_package_uuid_column_in_dataframe


def populate_dataframes_dictionary_parent_package_columns(
        dictionary_of_dataframes: dict) \
        -> dict:
    uuidified_packages_dataframe = \
        dictionary_of_dataframes[
            UUIDIFIED_PACKAGES_TABLE_NAME].copy()

    for dataframe_name, dataframe in dictionary_of_dataframes.items():
        dictionary_of_dataframes[dataframe_name] = \
            populate_parent_package_uuid_column_in_dataframe(
                dataframe_name=dataframe_name,
                dataframe=dataframe,
                uuidified_packages_dataframe=uuidified_packages_dataframe)

    return \
        dictionary_of_dataframes
