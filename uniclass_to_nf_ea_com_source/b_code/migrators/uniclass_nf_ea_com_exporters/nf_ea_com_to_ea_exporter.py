import os
from nf_common_source.code.services.file_system_service.objects.folders import Folders
from nf_common_source.code.services.reporting_service.reporters.log_with_datetime import log_message
from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.nf_ea_com_universes import NfEaComUniverses
from nf_ea_common_tools_source.b_code.services.general.nf_ea.domain_migration.domain_to_nf_ea_com_migration.orchestrators.nf_ea_com_universe_to_eapx_migration_orchestator import \
    orchestrate_nf_ea_com_universe_to_eapx_migration
from nf_ea_common_tools_source.b_code.services.session.orchestrators.ea_tools_session_managers import \
    EaToolsSessionManagers


def export_nf_ea_com_to_ea(
        ea_tools_session_manager: EaToolsSessionManagers,
        nf_ea_com_universe: NfEaComUniverses,
        folder_path: str):
    bclearer_stage = \
        nf_ea_com_universe.ea_repository.short_name

    current_stage_ea_export_folder_path = \
        os.path.join(
            folder_path,
            bclearer_stage,
            bclearer_stage + '_ea_export')

    if not os.path.exists(current_stage_ea_export_folder_path):
        os.mkdir(
            current_stage_ea_export_folder_path)

    log_message(
        'STARTING EA EXPORT (XML) FOR ' + bclearer_stage)

    orchestrate_nf_ea_com_universe_to_eapx_migration(
        ea_tools_session_manager=ea_tools_session_manager,
        nf_ea_com_universe=nf_ea_com_universe,
        short_name=bclearer_stage,
        output_folder=Folders(
            absolute_path_string=folder_path))

    log_message(
        'EA EXPORT DONE (XML) - ' + bclearer_stage)
