from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.dataframes.nf_ea_com_table_appender import \
    append_nf_ea_com_table


def convert_uniclass_stereotypes_table_to_stereotypes(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        input_stereotypes_table_name: str,
        nf_ea_com_stereotypes_collection_type: NfEaComCollectionTypes) \
        -> dict:
    uniclass_stereotypes_table = \
        uniclass_dictionary[input_stereotypes_table_name]

    nf_ea_com_dictionary = \
        append_nf_ea_com_table(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            new_nf_ea_com_collection=uniclass_stereotypes_table,
            nf_ea_com_collection_type=nf_ea_com_stereotypes_collection_type)

    return \
        nf_ea_com_dictionary
