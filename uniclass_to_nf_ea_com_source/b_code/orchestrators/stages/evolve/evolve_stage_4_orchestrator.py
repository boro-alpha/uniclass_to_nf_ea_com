from nf_common_source.code.services.reporting_service.wrappers.run_and_log_function_wrapper import run_and_log_function

from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_domain_to_nf_ea_com.evolve.evolve_stage_4.nf_ea_com_tables_creation_for_evolve_stage_4_orchestrator import \
    orchestrate_nf_ea_com_tables_creation_for_evolve_stage_4
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_nf_ea_com_exporters.export_nf_ea_com_orchestrator import \
    orchestrate_export_nf_ea_com
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.evolve_stage_4_domain_tables_generation_orchestrator import \
    orchestrate_domain_tables_creation_for_evolve_4


@run_and_log_function
def orchestrate_evolve_stage_4(
        dictionary_of_dataframes: dict,
        folder_path: str) \
        -> dict:

    evolve_stage_4_domain_tables = \
        orchestrate_domain_tables_creation_for_evolve_4(
            folder_path=folder_path,
            dictionary_of_dataframes=dictionary_of_dataframes)

    evolve_stage_4_nf_ea_com_tables = \
        orchestrate_nf_ea_com_tables_creation_for_evolve_stage_4(
            dictionary_of_dataframes=evolve_stage_4_domain_tables)

    orchestrate_export_nf_ea_com(
        nf_ea_com_dictionary=evolve_stage_4_nf_ea_com_tables,
        output_folder_path=folder_path,
        bclearer_stage='evolve_4')

    return \
        evolve_stage_4_domain_tables
