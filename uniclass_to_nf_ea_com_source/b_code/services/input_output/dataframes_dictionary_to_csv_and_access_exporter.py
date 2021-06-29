import os
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.input_output_service.access.access_database_creator import \
    create_access_database_in_folder
from nf_common_source.code.services.input_output_service.access.dataframes_to_access_writer import \
    write_dataframes_to_access


def export_dataframes_dictionary_to_csv_and_access(
        dictionary_of_dataframes: dict,
        folder_path: str,
        database_name: str):
    csv_folder_path = \
        __get_csv_folder_path(
            folder_path=folder_path) + '\\'

    access_database_full_path = \
        create_access_database_in_folder(
            parent_folder_path=folder_path,
            database_name=database_name)

    write_dataframes_to_access(
        dataframes_dictionary_keyed_on_string=dictionary_of_dataframes,
        database_file=Files(absolute_path_string=access_database_full_path),
        temporary_csv_folder=Folders(absolute_path_string=csv_folder_path))


def __get_csv_folder_path(
        folder_path: str) \
        -> str:
    csv_folder_path = \
        os.path.join(
            folder_path,
            'csvs')

    try:
        os.mkdir(
            csv_folder_path)

    except FileExistsError:
        print(
            'Target folder already exists.')

    return \
        csv_folder_path
