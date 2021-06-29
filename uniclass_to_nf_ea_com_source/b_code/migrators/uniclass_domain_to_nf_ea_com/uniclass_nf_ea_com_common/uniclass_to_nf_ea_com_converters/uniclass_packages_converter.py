import numpy
import pandas
from nf_common_source.code.nf.types.nf_column_types import NfColumnTypes
from nf_common_source.code.services.identification_services.uuid_service.uuid_helpers.uuid_factory import \
    create_new_uuid
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.column_types.nf_ea_com_column_types import \
    NfEaComColumnTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.dataframes.nf_ea_com_table_appender import \
    append_nf_ea_com_table


def convert_dictionary_keys_to_packages(
        uniclass_dictionary: dict,
        nf_ea_com_dictionary: dict,
        nf_ea_com_packages_collection_type: NfEaComCollectionTypes) \
        -> dict:
    nf_uuids_column_name = \
        NfColumnTypes.NF_UUIDS.column_name

    ea_object_type_column_name = \
        NfEaComColumnTypes.ELEMENTS_EA_OBJECT_TYPE.column_name

    ea_object_name_column_name = \
        NfEaComColumnTypes.EXPLICIT_OBJECTS_EA_OBJECT_NAME.column_name

    nf_uuids_list = []

    object_types_list = []

    object_names_list = []

    for dataframe_key in uniclass_dictionary.keys():
        nf_uuids_list.append(
                create_new_uuid())

        object_types_list.append(
                'Package')

        object_names_list.append(
                dataframe_key)

    ea_packages_dataframe_values_dictionary = {
        nf_uuids_column_name: nf_uuids_list,
        ea_object_type_column_name: object_types_list,
        ea_object_name_column_name: object_names_list
    }

    ea_packages_dataframe_values = \
        pandas.DataFrame(
            data=ea_packages_dataframe_values_dictionary)

    ea_packages_dataframe_values[NfEaComColumnTypes.PACKAGEABLE_OBJECTS_PARENT_EA_ELEMENT.column_name] = \
        numpy.nan

    nf_ea_com_dictionary = \
        append_nf_ea_com_table(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            new_nf_ea_com_collection=ea_packages_dataframe_values,
            nf_ea_com_collection_type=nf_ea_com_packages_collection_type)

    return \
        nf_ea_com_dictionary
