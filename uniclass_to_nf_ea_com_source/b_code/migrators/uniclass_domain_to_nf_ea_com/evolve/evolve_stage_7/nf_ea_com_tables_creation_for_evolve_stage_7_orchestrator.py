from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.nf_ea_com_initialiser import \
    initialise_nf_ea_com_dictionary

from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_classifiers_converter import \
    convert_standard_object_table_to_classifiers
from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_connectors_converter import \
    convert_standard_linked_table_to_connectors

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UUIDIFIED_PACKAGES_TABLE_NAME
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_attributes_converter import \
    convert_uniclass_naming_spaces_table_to_attributes
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_attributes_order_converter import \
    convert_uniclass_naming_spaces_table_to_attributes_order
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_classifiers_converter import \
    convert_uniclass_table_to_classifiers_in_common_package
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_connectors_converter import \
    convert_uniclass_items_parent_child_table_to_connectors
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_stereotype_usage_converter import \
    convert_uniclass_items_parent_child_table_to_stereotype_usage
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_stereotypes_converter import \
    convert_uniclass_stereotypes_table_to_stereotypes


def orchestrate_nf_ea_com_tables_creation_for_evolve_stage_7(
        dictionary_of_dataframes: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        initialise_nf_ea_com_dictionary()

    nf_ea_com_dictionary = \
        __convert_domain_evolve_stage_7(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=dictionary_of_dataframes)

    return \
        nf_ea_com_dictionary


def __convert_domain_evolve_stage_7(
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
        __convert_connectors(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=uniclass_dictionary)

    nf_ea_com_dictionary = \
        __convert_stereotypes(
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

    nf_ea_com_dictionary = \
        __convert_stereotype_usage(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=uniclass_dictionary)

    return \
        nf_ea_com_dictionary


def __convert_packages(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
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
            uniclass_table=uniclass_dictionary['uniclass2015_object_table'],
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    nf_ea_com_dictionary = \
        convert_standard_object_table_to_classifiers(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_object_table_name='uniclass_naming_spaces_objects',
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    nf_ea_com_dictionary = \
        convert_standard_object_table_to_classifiers(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_object_table_name='uniclass2015_ranks_table',
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    return \
        nf_ea_com_dictionary


def __convert_connectors(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_items_parent_child_table_to_connectors(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name='uniclass2015_parent_child_link_table',
            nf_ea_com_connectors_collection_type=NfEaComCollectionTypes.EA_CONNECTORS)

    nf_ea_com_dictionary = \
        convert_standard_linked_table_to_connectors(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name='linked_table_uniclass_items_to_ranks',
            nf_ea_com_connectors_collection_type=NfEaComCollectionTypes.EA_CONNECTORS,
            needs_uuid_generation=False)

    return \
        nf_ea_com_dictionary


def __convert_stereotypes(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_stereotypes_table_to_stereotypes(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_stereotypes_table_name='uniclass_stereotypes_type_of_table',
            nf_ea_com_stereotypes_collection_type=NfEaComCollectionTypes.EA_STEREOTYPES)

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
            input_naming_spaces_table_name='uniclass_naming_spaces_objects',
            ea_attributes_order_table_name='ea_attributes_order')

    return \
        nf_ea_com_dictionary


def __convert_attributes(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_naming_spaces_table_to_attributes(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_naming_spaces_table_name='uniclass_naming_spaces_objects',
            uniclass_items_object_table_name='uniclass2015_object_table',
            ea_attributes_collection_type=NfEaComCollectionTypes.EA_ATTRIBUTES)

    return \
        nf_ea_com_dictionary


def __convert_stereotype_usage(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_items_parent_child_table_to_stereotype_usage(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_uniclass_parent_child_table_name='uniclass2015_parent_child_link_table',
            input_stereotypes_table_name='uniclass_stereotypes_type_of_table',
            nf_ea_com_stereotype_usage_collection_type=NfEaComCollectionTypes.STEREOTYPE_USAGE)

    return \
        nf_ea_com_dictionary
