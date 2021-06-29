from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_uuidifier import uuidify_dataframe
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import StandardObjectTableColumnTypes
from pandas import DataFrame
from pandas import read_csv


def get_uuidified_ranks_table(
        csv_file_path: str) \
        -> DataFrame:

    ranks_table = \
        read_csv(
            csv_file_path)

    uuidified_ranks_table = \
        uuidify_dataframe(
            dataframe=ranks_table,
            uuid_column_name=StandardObjectTableColumnTypes.NF_UUIDS.column_name)

    return \
        uuidified_ranks_table
