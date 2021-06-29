from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.common_knowledge.collection_types.nf_ea_com_collection_types import \
    NfEaComCollectionTypes
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.processes.nf_ea_com_initialiser import \
    initialise_nf_ea_com_dictionary

from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_classifiers_converter import \
    convert_uniclass_table_to_classifiers_in_its_own_package
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.uniclass_nf_ea_com_common.uniclass_to_nf_ea_com_converters.uniclass_packages_converter import \
    convert_dictionary_keys_to_packages


def orchestrate_nf_ea_com_tables_creation_for_evolve_stage_1(
    dictionary_of_dataframes: dict) \
            -> dict:
    nf_ea_com_dictionary = \
        initialise_nf_ea_com_dictionary()

    nf_ea_com_dictionary = \
        __convert_domain_evolve_stage_1(
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            uniclass_dictionary=dictionary_of_dataframes)

    return \
        nf_ea_com_dictionary


def __convert_domain_evolve_stage_1(
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

    return \
        nf_ea_com_dictionary


def __convert_packages(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict)\
        -> dict:
    nf_ea_com_dictionary = \
        convert_dictionary_keys_to_packages(
            uniclass_dictionary=uniclass_dictionary,
            nf_ea_com_dictionary=nf_ea_com_dictionary,
            nf_ea_com_packages_collection_type=NfEaComCollectionTypes.EA_PACKAGES)

    return \
        nf_ea_com_dictionary


def __convert_classifiers(
        nf_ea_com_dictionary: dict,
        uniclass_dictionary: dict)\
        -> dict:
    for uniclass_table_name, uniclass_table in uniclass_dictionary.items():
        nf_ea_com_dictionary = \
            convert_uniclass_table_to_classifiers_in_its_own_package(
                uniclass_table_name=uniclass_table_name,
                uniclass_table=uniclass_table,
                nf_ea_com_dictionary=nf_ea_com_dictionary,
                nf_ea_com_classifiers_collection_type=NfEaComCollectionTypes.EA_CLASSIFIERS)

    return \
        nf_ea_com_dictionary
