from nf_common_source.code.services.file_system_service.new_folder_creator import create_new_folder

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import LOAD_STAGE_4_NAME
from uniclass_to_nf_ea_com_source.b_code.services.input_output.dataframes_dictionary_to_csv_and_access_exporter import \
    export_dataframes_dictionary_to_csv_and_access


def export_load_stage_4_domain_tables(
        folder_path: str,
        dictionary_of_dataframes: dict,
        database_name: str):
    load_stage_4_folder_path = \
        create_new_folder(
            parent_folder_path=folder_path,
            new_folder_name=LOAD_STAGE_4_NAME)

    load_stage_4_domain_tables_folder_path = \
        create_new_folder(
            parent_folder_path=load_stage_4_folder_path,
            new_folder_name=LOAD_STAGE_4_NAME + '_domain_tables')

    export_dataframes_dictionary_to_csv_and_access(
        dictionary_of_dataframes=dictionary_of_dataframes,
        folder_path=load_stage_4_domain_tables_folder_path,
        database_name=database_name)
