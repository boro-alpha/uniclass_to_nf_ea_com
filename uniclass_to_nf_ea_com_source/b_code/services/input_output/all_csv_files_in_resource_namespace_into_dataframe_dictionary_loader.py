import importlib
from os.path import splitext, basename
import pandas
from nf_common_source.code.services.file_system_service.files_of_extension_from_folder_getter import \
    get_all_files_of_extension_from_folder
from nf_common_source.code.services.file_system_service.objects.files import Files
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import CSV_EXTENSION_FILE_NAME


def load_all_csv_files_in_resource_namespace_into_dataframe_dictionary(
        resource_namespace: str) \
        -> dict:
    dictionary_of_dataframes = \
        dict()

    module = \
        importlib.import_module(
            name=resource_namespace)

    module_path_string = \
        module.__path__[0]

    resource_namespace_folder = \
        Folders(
            absolute_path_string=module_path_string)

    csv_files = \
        get_all_files_of_extension_from_folder(
            folder=resource_namespace_folder,
            dot_extension_string=CSV_EXTENSION_FILE_NAME)

    for csv_file in csv_files:
        dictionary_of_dataframes = \
            __add_csv_file_to_dictionary_of_dataframes(
                csv_file=csv_file,
                dictionary_of_dataframes=dictionary_of_dataframes)

    return \
        dictionary_of_dataframes


def __add_csv_file_to_dictionary_of_dataframes(
        csv_file: Files,
        dictionary_of_dataframes: dict) \
        -> dict:
    base_file_name_without_extension = \
        splitext(
            basename(
                csv_file.absolute_path_string))[0]

    dataframe = \
        pandas.read_csv(
            filepath_or_buffer=csv_file.absolute_path_string,
            dtype=str,
            encoding='cp1252')

    dictionary_of_dataframes.update({
            base_file_name_without_extension: dataframe})

    return \
        dictionary_of_dataframes
