from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.nf_ea_com_initialiser import \
    initialise_nf_ea_com_dictionary

from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_classifiers_converter import \
    convert_standard_object_table_to_classifiers

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME, \
    UNICLASS2015_OBJECT_TABLE_NAME, UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_attributes_converter import \
    convert_uniclass_naming_spaces_table_to_attributes
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_attributes_order_converter import \
    convert_uniclass_naming_spaces_table_to_attributes_order
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_classifiers_converter import \
    convert_uniclass_table_to_classifiers_in_common_package


def orchestrate_nf_ea_com_tables_creation_for_evolve_stage_3(
        dictionary_of_dataframes: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        initialise_nf_ea_com_dictionary()

    nf_ea_com_dictionary = \
        __convert_domain_evolve_stage_3(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=dictionary_of_dataframes)

    return \
        nf_ea_com_dictionary


def __convert_domain_evolve_stage_3(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        __convert_packages(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=uniclass_dictionary)

    nf_ea_com_dictionary = \
        __convert_classifiers(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=uniclass_dictionary)

    nf_ea_com_dictionary = \
        __convert_attributes_order(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=uniclass_dictionary)

    nf_ea_com_dictionary = \
        __convert_attributes(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=uniclass_dictionary)

    return \
        nf_ea_com_dictionary


def __convert_packages(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict)\
        -> dict:
    nf_ea_com_dictionary[NfEaComCollectionTypes.EA_PACKAGES] = \
        uniclass_dictionary[UUIDIFIED_PACKAGES_TABLE_NAME]

    return \
        nf_ea_com_dictionary


def __convert_classifiers(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_table_to_classifiers_in_common_package(
            uniclass_table=uniclass_dictionary[UNICLASS2015_OBJECT_TABLE_NAME],
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    nf_ea_com_dictionary = \
        convert_standard_object_table_to_classifiers(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_object_table_name=UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME,
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    return \
        nf_ea_com_dictionary


def __convert_attributes_order(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_naming_spaces_table_to_attributes_order(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_naming_spaces_table_name=UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME,
            ea_attributes_order_table_name='ea_attributes_order')

    return \
        nf_ea_com_dictionary


def __convert_attributes(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_naming_spaces_table_to_attributes(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_naming_spaces_table_name=UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME,
            uniclass_items_object_table_name=UNICLASS2015_OBJECT_TABLE_NAME,
            ea_attributes_collection_type=NfEaComCollectionTypes.EA_ATTRIBUTES)

    return \
        nf_ea_com_dictionary
