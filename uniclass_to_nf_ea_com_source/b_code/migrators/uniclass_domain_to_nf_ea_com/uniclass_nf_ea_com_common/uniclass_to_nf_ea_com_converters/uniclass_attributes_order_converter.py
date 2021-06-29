import pandas
from nf_common_source.code.nf.types.nf_column_types import NfColumnTypes
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes


def convert_uniclass_naming_spaces_table_to_attributes_order(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        input_naming_spaces_table_name: str,
        ea_attributes_order_table_name: str) \
        -> dict:
    uniclass_naming_spaces_to_ea_attributes_order_renaming_dictionary = {
        StandardObjectTableColumnTypes.NF_UUIDS.column_name: NfColumnTypes.NF_UUIDS.column_name,
        StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name: 'naming_space_names',
        'attribute_order': 'attribute_order'}

    uniclass_naming_spaces_for_attribute_order_filtered_and_renamed = \
        dataframe_filter_and_rename(
            dataframe=uniclass_dictionary[input_naming_spaces_table_name],
            filter_and_rename_dictionary=uniclass_naming_spaces_to_ea_attributes_order_renaming_dictionary)

    ea_attributes_order_dataframe = \
        pandas.concat([
            uniclass_naming_spaces_for_attribute_order_filtered_and_renamed,
            nf_ea_com_dictionary[ea_attributes_order_table_name]
        ])

    nf_ea_com_dictionary[ea_attributes_order_table_name] = \
        ea_attributes_order_dataframe

    return \
        nf_ea_com_dictionary
