from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.evolve.evolve_stage_5.nf_ea_com_tables_creation_for_evolve_stage_5_orchestrator import \
    orchestrate_nf_ea_com_tables_creation_for_evolve_stage_5
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_nf_ea_com_exporters.export_nf_ea_com_orchestrator import \
    orchestrate_export_nf_ea_com
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_5.evolve_stage_5_domain_tables_generation_orchestrator import \
    orchestrate_domain_tables_creation_for_evolve_5


@run_and_log_function
def orchestrate_evolve_stage_5(
        dictionary_of_dataframes: dict,
        folder_path: str) \
        -> dict:
    uniclass2015_object_parent_child_tables_with_uniclass_element = \
        orchestrate_domain_tables_creation_for_evolve_5(
            folder_path=folder_path,
            dictionary_of_dataframes=dictionary_of_dataframes)

    evolve_stage_5_nf_ea_com_tables = \
        orchestrate_nf_ea_com_tables_creation_for_evolve_stage_5(
            dictionary_of_dataframes=uniclass2015_object_parent_child_tables_with_uniclass_element)

    orchestrate_export_nf_ea_com(
        nf_ea_com_dictionary=evolve_stage_5_nf_ea_com_tables,
        output_folder_path=folder_path,
        bclearer_stage='evolve_5')

    return \
        uniclass2015_object_parent_child_tables_with_uniclass_element
