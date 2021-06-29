from nf_common_source.code.nf.types.nf_column_types import NfColumnTypes
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.column_types.nf_ea_com_column_types import \
    NfEaComColumnTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.dataframes.nf_ea_com_table_appender import \
    append_nf_ea_com_table


def convert_uniclass_items_parent_child_table_to_stereotype_usage(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        input_uniclass_parent_child_table_name: str,
        input_stereotypes_table_name: str,
        nf_ea_com_stereotype_usage_collection_type: NfEaComCollectionTypes) \
        -> dict:
    uniclass_parent_child_dataframe = \
        uniclass_dictionary[input_uniclass_parent_child_table_name]

    ea_stereotype_usage_dataframe_filter_and_renaming_dictionary = {
        NfColumnTypes.NF_UUIDS.column_name: 'client_nf_uuids'
    }

    ea_stereotype_usage_dataframe = \
        dataframe_filter_and_rename(
            dataframe=uniclass_parent_child_dataframe,
            filter_and_rename_dictionary=ea_stereotype_usage_dataframe_filter_and_renaming_dictionary)

    ea_stereotypes_table = \
        uniclass_dictionary[input_stereotypes_table_name]

    stereotype_type_of_nf_uuid =  \
        ea_stereotypes_table.loc[
            ea_stereotypes_table[NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name] == 'type-of',
            NfColumnTypes.NF_UUIDS.column_name].to_string(index=False).strip()

    ea_stereotype_usage_dataframe['stereotype_nf_uuids'] = \
        stereotype_type_of_nf_uuid

    nf_ea_com_dictionary = \
        append_nf_ea_com_table(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            new_nf_ea_com_collection=ea_stereotype_usage_dataframe,
            nf_ea_com_collection_type=nf_ea_com_stereotype_usage_collection_type)

    return \
        nf_ea_com_dictionary
