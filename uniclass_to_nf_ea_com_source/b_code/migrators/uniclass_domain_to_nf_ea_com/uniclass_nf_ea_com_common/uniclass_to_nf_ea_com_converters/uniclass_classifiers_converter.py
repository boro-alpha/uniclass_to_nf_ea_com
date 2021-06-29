import pandas
from nf_common_source.code.nf.types.nf_column_types import NfColumnTypes
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.column_types.nf_ea_com_column_types import \
    NfEaComColumnTypes

from uniclass_to_nf_ea_com_source.b_code.configurations.objects.uniclass_namespace_ckids import UniclassNamespaceCkIds


def convert_uniclass_table_to_classifiers_in_common_package(
        uniclass_table: pandas.DataFrame,
        nf_ea_com_dictionary: dict,
        nf_ea_com_classifiers_collection_type: NfEaComCollectionTypes) \
        -> dict:
    nf_ea_com_dictionary = \
        __convert_uniclass_table_to_classifiers_in_package(
            uniclass_table=uniclass_table,
            package_name='UNICLASS Items',
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            nf_ea_com_classifiers_collection_type=nf_ea_com_classifiers_collection_type)

    return \
        nf_ea_com_dictionary


def convert_uniclass_table_to_classifiers_in_its_own_package(
        uniclass_table_name: str,
        uniclass_table: pandas.DataFrame,
        nf_ea_com_dictionary: dict,
        nf_ea_com_classifiers_collection_type: NfEaComCollectionTypes) \
        -> dict:
    nf_ea_com_dictionary = \
        __convert_uniclass_table_to_classifiers_in_package(
            uniclass_table=uniclass_table,
            package_name=uniclass_table_name,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            nf_ea_com_classifiers_collection_type=nf_ea_com_classifiers_collection_type)

    return \
        nf_ea_com_dictionary


def __convert_uniclass_table_to_classifiers_in_package(
        uniclass_table: pandas.DataFrame,
        package_name: str,
        nf_ea_com_dictionary: dict,
        nf_ea_com_classifiers_collection_type: NfEaComCollectionTypes) \
        -> dict:
    ea_packages_dataframe = \
        nf_ea_com_dictionary[NfEaComCollectionTypes.EA_PACKAGES]

    uniclass_items_package_nf_uuid = \
        ea_packages_dataframe.loc[
            ea_packages_dataframe[
                NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name] == package_name,
            NfColumnTypes.NF_UUIDS.column_name].to_string(index=False).strip()

    uniclass_table_columns = \
        uniclass_table.columns.tolist()

    if StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name in uniclass_table_columns:
        uniclass_table_renaming_dictionary = {
            StandardObjectTableColumnTypes.NF_UUIDS.column_name: NfColumnTypes.NF_UUIDS.column_name,
            StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name: NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name
        }
    else:
        uniclass_table_renaming_dictionary = {
            StandardObjectTableColumnTypes.NF_UUIDS.column_name: NfColumnTypes.NF_UUIDS.column_name,
            UniclassNamespaceCkIds.CODE.to_string(): NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name
        }

    uniclass_table_filtered_and_renamed = \
        dataframe_filter_and_rename(
            dataframe=uniclass_table,
            filter_and_rename_dictionary=uniclass_table_renaming_dictionary)

    uniclass_table_filtered_and_renamed[NfEaComColumnTypes.ELEMENTS_EA_OBJECT_TYPE.column_name] = \
        'Class'

    uniclass_table_filtered_and_renamed[NfEaComColumnTypes.PACKAGEABLE_OBJECTS_PARENT_EA_ELEMENT.column_name] = \
        uniclass_items_package_nf_uuid

    nf_ea_com_collection = \
        pandas.concat(
            [nf_ea_com_dictionary[nf_ea_com_classifiers_collection_type],
             uniclass_table_filtered_and_renamed])

    nf_ea_com_dictionary[nf_ea_com_classifiers_collection_type] = \
        nf_ea_com_collection

    return \
        nf_ea_com_dictionary
