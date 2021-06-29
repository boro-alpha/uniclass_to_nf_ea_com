from nf_common_source.code.services.file_system_service.new_folder_creator import create_new_folder

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import EVOLVE_STAGE_6_NAME, \
    UNICLASS_EVOLVE_STAGE_6_DOMAIN_TABLES
from uniclass_to_nf_ea_com_source.b_code.services.input_output.dataframes_dictionary_to_csv_and_access_exporter import \
    export_dataframes_dictionary_to_csv_and_access
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_6.domain_tables_data_processor.evolve_stage_6_domain_tables_getter import \
    get_evolve_stage_6_domain_tables


def orchestrate_domain_tables_creation_for_evolve_6(
        folder_path: str,
        dictionary_of_dataframes: dict) \
        -> dict:
    evolve_stage_6_domain_tables = \
        get_evolve_stage_6_domain_tables(
            dictionary_of_dataframes=dictionary_of_dataframes)

    evolve_stage_6_folder_path = \
        create_new_folder(
            parent_folder_path=folder_path,
            new_folder_name=EVOLVE_STAGE_6_NAME)

    evolve_stage_6_domain_tables_folder_path = \
        create_new_folder(
            parent_folder_path=evolve_stage_6_folder_path,
            new_folder_name=EVOLVE_STAGE_6_NAME + '_domain_tables')

    export_dataframes_dictionary_to_csv_and_access(
        dictionary_of_dataframes=evolve_stage_6_domain_tables,
        folder_path=evolve_stage_6_domain_tables_folder_path,
        database_name=UNICLASS_EVOLVE_STAGE_6_DOMAIN_TABLES)
    return \
        evolve_stage_6_domain_tables
