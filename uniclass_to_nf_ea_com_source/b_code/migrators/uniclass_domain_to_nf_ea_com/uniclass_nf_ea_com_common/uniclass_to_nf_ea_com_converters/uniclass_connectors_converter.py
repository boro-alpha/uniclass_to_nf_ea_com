import numpy
from nf_common_source.code.nf.types.nf_column_types import NfColumnTypes
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.column_types.nf_ea_com_column_types import \
    NfEaComColumnTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.dataframes.nf_ea_com_table_appender import \
    append_nf_ea_com_table

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CHILD_UUID_COLUMN_NAME, \
    PARENT_UUID_COLUMN_NAME


def convert_uniclass_items_parent_child_table_to_connectors(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        input_linked_table_name: str,
        nf_ea_com_connectors_collection_type: NfEaComCollectionTypes) \
        -> dict:
    ea_connectors_nf_uuids_column_name = \
        NfColumnTypes.NF_UUIDS.column_name

    uniclass_items_parent_child_filter_and_rename_dictionary = {
        ea_connectors_nf_uuids_column_name: ea_connectors_nf_uuids_column_name,
        CHILD_UUID_COLUMN_NAME: NfEaComColumnTypes.ELEMENTS_SUPPLIER_PLACE1_END_CONNECTORS.column_name,
        PARENT_UUID_COLUMN_NAME: NfEaComColumnTypes.ELEMENTS_CLIENT_PLACE2_END_CONNECTORS.column_name}

    uniclass_items_parent_child_table_filtered_and_renamed = \
        dataframe_filter_and_rename(
            dataframe=uniclass_dictionary[input_linked_table_name],
            filter_and_rename_dictionary=uniclass_items_parent_child_filter_and_rename_dictionary)

    uniclass_items_parent_child_table_filtered_and_renamed[NfEaComColumnTypes.CONNECTORS_ELEMENT_TYPE_NAME.column_name] = \
        'Association'

    ea_connectors_ea_object_name_column_name = \
        NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name

    uniclass_items_parent_child_table_filtered_and_renamed[ea_connectors_ea_object_name_column_name] = \
        numpy.nan

    nf_ea_com_dictionary = \
        append_nf_ea_com_table(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            new_nf_ea_com_collection=uniclass_items_parent_child_table_filtered_and_renamed,
            nf_ea_com_collection_type=nf_ea_com_connectors_collection_type)

    return \
        nf_ea_com_dictionary
