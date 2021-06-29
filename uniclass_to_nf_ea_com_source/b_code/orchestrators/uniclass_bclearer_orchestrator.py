import os
from nf_common_source.code.services.datetime_service.time_helpers.time_getter import now_time_as_string_for_files
from nf_common_source.code.services.file_system_service.folder_selector import select_folder
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_1_orchestrator import orchestrate_evolve_stage_1
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_2_orchestrator import orchestrate_evolve_stage_2
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_3_orchestrator import orchestrate_evolve_stage_3
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_4_orchestrator import orchestrate_evolve_stage_4
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_5_orchestrator import orchestrate_evolve_stage_5
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_7_orchestrator import orchestrate_evolve_stage_7
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_8_orchestrator import orchestrate_evolve_stage_8
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.evolve.evolve_stage_6_orchestrator import orchestrate_evolve_stage_6
from uniclass_to_nf_ea_com_source.b_code.orchestrators.stages.load.load_stage_4_orchestrator import orchestrate_load_stage_4
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import ROOT_OUTPUT_FOLDER_TITLE_MESSAGE
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import UNICLASS_BCLEARER_PREFIX_FOR_NAMES
from uniclass_to_nf_ea_com_source.b_code.services.input_output.set_up_and_close_out_logging import set_up_logger_and_output_folder
from uniclass_to_nf_ea_com_source.b_code.services.input_output.set_up_and_close_out_logging import close_log_file


def orchestrate_uniclass_bclearer(
        uniclass_source_data_resource_namespace: str):
    root_folder_path = \
        __get_output_root_folder_path()

    set_up_logger_and_output_folder(
        output_folder_name=root_folder_path)

    load_stage_4_dictionary_of_dataframes = \
        __orchestrate_uniclass_load_stage(
            folder_path=root_folder_path,
            uniclass_source_data_resource_namespace=uniclass_source_data_resource_namespace)

    __orchestrate_uniclass_evolve_stage(
        load_stage_4_dictionary_of_dataframes=load_stage_4_dictionary_of_dataframes,
        folder_path=root_folder_path)

    close_log_file()


def __get_output_root_folder_path() \
        -> str:
    root_folder = \
        select_folder(
            title=ROOT_OUTPUT_FOLDER_TITLE_MESSAGE)

    root_folder_path = \
        root_folder.absolute_path_string

    output_root_folder_path = \
        os.path.join(
            root_folder_path,
            UNICLASS_BCLEARER_PREFIX_FOR_NAMES + now_time_as_string_for_files().replace('_', ''))

    os.mkdir(
        output_root_folder_path)

    return \
        output_root_folder_path


def __orchestrate_uniclass_load_stage(
        folder_path: str,
        uniclass_source_data_resource_namespace: str) \
        -> dict:
    load_stage_4_dictionary_of_dataframes = \
        orchestrate_load_stage_4(
            folder_path=folder_path,
            uniclass_source_data_resource_namespace=uniclass_source_data_resource_namespace)

    return \
        load_stage_4_dictionary_of_dataframes


def __orchestrate_uniclass_evolve_stage(
        load_stage_4_dictionary_of_dataframes: dict,
        folder_path: str):
    evolve_stage_1_dictionary_of_dataframes = \
        orchestrate_evolve_stage_1(
            dictionary_of_dataframes=load_stage_4_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_2_dictionary_of_dataframes = \
        orchestrate_evolve_stage_2(
            dictionary_of_dataframes=evolve_stage_1_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_3_dictionary_of_dataframes = \
        orchestrate_evolve_stage_3(
            dictionary_of_dataframes=evolve_stage_2_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_4_dictionary_of_dataframes = \
        orchestrate_evolve_stage_4(
            dictionary_of_dataframes=evolve_stage_3_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_5_dictionary_of_dataframes = \
        orchestrate_evolve_stage_5(
            dictionary_of_dataframes=evolve_stage_4_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_6_dictionary_of_dataframes = \
        orchestrate_evolve_stage_6(
            dictionary_of_dataframes=evolve_stage_5_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_7_dictionary_of_dataframes = \
        orchestrate_evolve_stage_7(
            dictionary_of_dataframes=evolve_stage_6_dictionary_of_dataframes,
            folder_path=folder_path)

    evolve_stage_8_dictionary_of_dataframes = \
        orchestrate_evolve_stage_8(
            evolve_stage_7_dictionary_of_dataframes=evolve_stage_7_dictionary_of_dataframes,
            folder_path=folder_path)
