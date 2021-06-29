from nf_ea_common_tools_source.b_code.services.general.nf_ea.com.temporary.nf_ea_com_dictionary_to_nf_ea_com_universe_convertor import \
    convert_nf_ea_com_dictionary_to_nf_ea_com_universe
from nf_ea_common_tools_source.b_code.services.session.orchestrators.ea_tools_session_managers import \
    EaToolsSessionManagers
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_nf_ea_com_exporters.nf_ea_com_to_access_exporter import \
    export_nf_ea_com_to_access
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_nf_ea_com_exporters.nf_ea_com_to_ea_exporter import \
    export_nf_ea_com_to_ea
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_nf_ea_com_exporters.nf_ea_com_to_hdf5_exporter import \
    export_nf_ea_com_to_hdf5


def orchestrate_export_nf_ea_com(
        nf_ea_com_dictionary: dict,
        output_folder_path: str,
        bclearer_stage: str):
    with EaToolsSessionManagers() as ea_tools_session_manager:
        nf_ea_com_universe = \
            convert_nf_ea_com_dictionary_to_nf_ea_com_universe(
                ea_tools_session_manager=ea_tools_session_manager,
                nf_ea_com_dictionary=nf_ea_com_dictionary,
                short_name=bclearer_stage)

        export_nf_ea_com_to_access(
            ea_tools_session_manager=ea_tools_session_manager,
            nf_ea_com_universe=nf_ea_com_universe,
            folder_path=output_folder_path)

        export_nf_ea_com_to_ea(
            ea_tools_session_manager=ea_tools_session_manager,
            nf_ea_com_universe=nf_ea_com_universe,
            folder_path=output_folder_path)

        # export_nf_ea_com_to_hdf5(
        #     nf_ea_com_universe=nf_ea_com_universe,
        #     folder_path=output_folder_path)
