from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.nf_ea_com_initialiser import \
    initialise_nf_ea_com_dictionary

from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_attributes_converter import \
    convert_standard_attribute_table_to_attributes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_classifiers_converter import \
    convert_standard_object_table_to_classifiers
from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_classifiers_proxy_connectors_converter import \
    convert_typed_linked_table_to_classifiers_proxy_connectors
from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_connectors_converter import \
    convert_standard_linked_table_to_connectors
from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.convertors.tables.standard_connectors_pc_converter import \
    convert_standard_typed_linked_table_to_connectors_pc

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    UUIDIFIED_PACKAGES_TABLE_NAME, RELATION_TYPE_UUIDS_COLUMN_NAME, UNICLASS2015_OBJECT_TABLE_NAME, \
    UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME, BCLEARER_FOUNDATION_OBJECTS_TABLE_NAME, ISO_12006_OBJECTS_TABLE_NAME, \
    PARENT_CHILD_SUBTYPES_TABLE_NAME, UNICLASS_TOP_LEVEL_CORE_OBJECTS_TABLE_NAME, UNICLASS2015_RANKS_TABLE_NAME, \
    CLASSIFICATION_RANKS_NAMES_TABLE_NAME
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


def orchestrate_nf_ea_com_tables_creation_for_evolve_stage_8(
        dictionary_of_dataframes: dict)\
        -> dict:
    nf_ea_com_dictionary = \
        initialise_nf_ea_com_dictionary()

    nf_ea_com_dictionary = \
        __convert_domain(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=dictionary_of_dataframes)

    return \
        nf_ea_com_dictionary


def __convert_domain(
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

    nf_ea_com_dictionary = \
        __convert_connectors_connecting_connectors(
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

    evolve_8_list_of_standard_object_table_names = [
        UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME,
        BCLEARER_FOUNDATION_OBJECTS_TABLE_NAME,
        ISO_12006_OBJECTS_TABLE_NAME,
        PARENT_CHILD_SUBTYPES_TABLE_NAME,
        UNICLASS_TOP_LEVEL_CORE_OBJECTS_TABLE_NAME,
        UNICLASS2015_RANKS_TABLE_NAME,
        CLASSIFICATION_RANKS_NAMES_TABLE_NAME
    ]

    for evolve_8_standard_object_table_name in evolve_8_list_of_standard_object_table_names:
        nf_ea_com_dictionary = \
            convert_standard_object_table_to_classifiers(
                standard_table_dictionary=uniclass_dictionary,
                nf_ea_com_dictionary=nf_ea_com_dictionary,
                input_object_table_name=evolve_8_standard_object_table_name,
                nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    return \
        nf_ea_com_dictionary


def __convert_connectors(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary:dict) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_items_parent_child_table_to_connectors(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name='uniclass2015_parent_child_link_table',
            nf_ea_com_connectors_collection_type=NfEaComCollectionTypes.EA_CONNECTORS)

    evolve_8_list_of_standard_linked_table_names = [
        'linked_table_uniclass_items_to_ranks',
        'linked_table_bclearer_foundation_package',
        'linked_table_iso_12006_package',
        'linked_table_namespaces_package',
        'linked_table_parent_child_subtypes_package',
        'linked_table_ranks_package',
        'linked_table_top_level_core_package',
        'linked_table_uniclass_item_to_object',
    ]

    for evolve_8_linked_table_names in evolve_8_list_of_standard_linked_table_names:
        nf_ea_com_dictionary = \
            convert_standard_linked_table_to_connectors(
                standard_table_dictionary=uniclass_dictionary,
                nf_ea_com_dictionary=nf_ea_com_dictionary,
                input_linked_table_name=evolve_8_linked_table_names,
                nf_ea_com_connectors_collection_type=NfEaComCollectionTypes.EA_CONNECTORS,
                needs_uuid_generation=True)

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

    nf_ea_com_dictionary = \
        convert_standard_attribute_table_to_attributes(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_attribute_table_name='attribute_table_higher_levels_name_spaces',
            ea_attributes_collection_type=NfEaComCollectionTypes.EA_ATTRIBUTES)

    return \
        nf_ea_com_dictionary


def __convert_stereotype_usage(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_uniclass_items_parent_child_table_to_stereotype_usage(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_uniclass_parent_child_table_name='uniclass2015_parent_child_link_table',
            input_stereotypes_table_name='uniclass_stereotypes_type_of_table',
            nf_ea_com_stereotype_usage_collection_type=NfEaComCollectionTypes.STEREOTYPE_USAGE)

    return \
        nf_ea_com_dictionary


def __convert_connectors_connecting_connectors(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict) \
        -> dict:
    nf_ea_com_dictionary = \
        __convert_typed_linked_table_to_proxy_connectors_and_connectors_pc(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name='uniclass2015_parent_child_link_table')

    nf_ea_com_dictionary = \
        __convert_typed_linked_table_to_proxy_connectors_and_connectors_pc(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name='linked_table_uniclass_items_to_ranks')

    return \
        nf_ea_com_dictionary


def __convert_typed_linked_table_to_proxy_connectors_and_connectors_pc(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict,
        input_linked_table_name: str) \
        -> dict:
    nf_ea_com_dictionary = \
        convert_typed_linked_table_to_classifiers_proxy_connectors(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name=input_linked_table_name,
            proxy_connectors_package_name='ProxyConnectors',
            ea_packages_collection_type=NfEaComCollectionTypes.EA_PACKAGES,
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    nf_ea_com_dictionary = \
        convert_standard_typed_linked_table_to_connectors_pc(
            standard_table_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            input_linked_table_name=input_linked_table_name,
            input_table_type_uuids_column_name=RELATION_TYPE_UUIDS_COLUMN_NAME,
            nf_ea_com_connectors_pc_collection_type=NfEaComCollectionTypes.EA_CONNECTORS_PC,
            nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    return \
        nf_ea_com_dictionary
