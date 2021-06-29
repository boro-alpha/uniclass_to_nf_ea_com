import pandas
from nf_common_source.code.nf.types.nf_column_types import NfColumnTypes
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_common_source.code.services.identification_services.uuid_service.uuid_helpers.uuid_factory import \
    create_new_uuid
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.column_types.nf_ea_com_column_types import \
    NfEaComColumnTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.dataframes.nf_ea_com_table_appender import \
    append_nf_ea_com_table

from uniclass_to_nf_ea_com_source.b_code.configurations.objects.uniclass_namespace_ckids import UniclassNamespaceCkIds


def convert_uniclass_naming_spaces_table_to_attributes(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        input_naming_spaces_table_name: str,
        uniclass_items_object_table_name: str,
        ea_attributes_collection_type: NfEaComCollectionTypes) \
        -> dict:
    uniclass_table_column_names_to_naming_space_names_dictionary = {
        UniclassNamespaceCkIds.CODE.to_string(): 'UNICLASS Item Composite Code',
        UniclassNamespaceCkIds.AREA.to_string(): 'UNICLASS Area Code',
        UniclassNamespaceCkIds.GROUP.to_string(): 'UNICLASS Group Component Code',
        UniclassNamespaceCkIds.SUB_GROUP.to_string(): 'UNICLASS Sub group Component Code',
        UniclassNamespaceCkIds.SECTION.to_string(): 'UNICLASS Section Component Code',
        UniclassNamespaceCkIds.OBJECT.to_string(): 'UNICLASS Object Component Code',
        UniclassNamespaceCkIds.TITLE.to_string(): 'UNICLASS Item Title'
    }

    for uniclass_column_name, naming_space_name in uniclass_table_column_names_to_naming_space_names_dictionary.items():
        nf_ea_com_dictionary = \
            __convert_uniclass_column_to_attributes(
                uniclass_dictionary=uniclass_dictionary,
                nf_ea_com_dictionary=nf_ea_com_dictionary,
                input_naming_spaces_table_name=input_naming_spaces_table_name,
                uniclass_items_object_table_name=uniclass_items_object_table_name,
                ea_attributes_collection_type=ea_attributes_collection_type,
                uniclass_column_name=uniclass_column_name,
                naming_space_name=naming_space_name)

    return \
        nf_ea_com_dictionary


def __convert_uniclass_column_to_attributes(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        input_naming_spaces_table_name: str,
        uniclass_items_object_table_name: str,
        ea_attributes_collection_type: NfEaComCollectionTypes,
        uniclass_column_name: str,
        naming_space_name: str)\
        -> dict:
    uniclass_items_sliced_filtered_and_renamed_for_ea_attributes = \
        __filter_uniclass_items_table_to_populated_column(
            uniclass_dictionary=uniclass_dictionary,
            uniclass_items_object_table_name=uniclass_items_object_table_name,
            uniclass_column_name=uniclass_column_name)

    naming_space_nf_uuid = \
        __get_naming_space_nf_uuid(
            uniclass_dictionary=uniclass_dictionary,
            input_naming_spaces_table_name=input_naming_spaces_table_name,
            naming_space_name=naming_space_name)

    ea_attribute_table = \
        __create_ea_attribute_table(
            uniclass_items_sliced_filtered_and_renamed_for_ea_attributes=uniclass_items_sliced_filtered_and_renamed_for_ea_attributes,
            naming_space_nf_uuid=naming_space_nf_uuid)

    nf_ea_com_dictionary = \
        append_nf_ea_com_table(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            new_nf_ea_com_collection=ea_attribute_table,
            nf_ea_com_collection_type=ea_attributes_collection_type)

    return \
        nf_ea_com_dictionary


def __filter_uniclass_items_table_to_populated_column(
        uniclass_dictionary: dict,
        uniclass_items_object_table_name: str,
        uniclass_column_name: str) \
        -> pandas.DataFrame:
    uniclass_items_dataframe = \
        uniclass_dictionary[uniclass_items_object_table_name]

    uniclass_items_sliced_to_column_value_not_null = \
        uniclass_items_dataframe[uniclass_items_dataframe[uniclass_column_name].notnull()]

    uniclass_items_filtered_and_rename_dictionary = {
        StandardObjectTableColumnTypes.NF_UUIDS.column_name: NfEaComColumnTypes.ELEMENT_COMPONENTS_CONTAINING_EA_CLASSIFIER.column_name,
        uniclass_column_name: NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name
    }

    uniclass_items_sliced_filtered_and_renamed_for_ea_attributes = \
        dataframe_filter_and_rename(
            dataframe=uniclass_items_sliced_to_column_value_not_null,
            filter_and_rename_dictionary=uniclass_items_filtered_and_rename_dictionary)

    return \
        uniclass_items_sliced_filtered_and_renamed_for_ea_attributes


def __get_naming_space_nf_uuid(
        uniclass_dictionary: dict,
        input_naming_spaces_table_name: str,
        naming_space_name: str) \
        -> str:
    naming_spaces_dataframe = \
        uniclass_dictionary[input_naming_spaces_table_name]

    naming_space_nf_uuid = \
        naming_spaces_dataframe.loc[
            naming_spaces_dataframe[StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name] == naming_space_name,
            StandardObjectTableColumnTypes.NF_UUIDS.column_name].to_string(index=False).strip()

    return \
        naming_space_nf_uuid


def __create_ea_attribute_table(
        uniclass_items_sliced_filtered_and_renamed_for_ea_attributes: pandas.DataFrame,
        naming_space_nf_uuid: str)\
        -> pandas.DataFrame:
    ea_attributes_classifying_ea_classifier_column_name = \
        NfEaComColumnTypes.ELEMENT_COMPONENTS_CLASSIFYING_EA_CLASSIFIER.column_name

    ea_attributes_uml_visibility_kind_column_name = \
        NfEaComColumnTypes.ELEMENT_COMPONENTS_UML_VISIBILITY_KIND.column_name

    ea_attributes_nf_uuids_column_name = \
        NfColumnTypes.NF_UUIDS.column_name

    uniclass_items_sliced_filtered_and_renamed_for_ea_attributes[ea_attributes_classifying_ea_classifier_column_name] = \
        naming_space_nf_uuid

    uniclass_items_sliced_filtered_and_renamed_for_ea_attributes[ea_attributes_uml_visibility_kind_column_name] = \
        'Public'

    uniclass_items_sliced_filtered_and_renamed_for_ea_attributes[ea_attributes_nf_uuids_column_name] = \
        uniclass_items_sliced_filtered_and_renamed_for_ea_attributes.apply(
            lambda row:
            create_new_uuid(),
            axis=1)

    return \
        uniclass_items_sliced_filtered_and_renamed_for_ea_attributes
