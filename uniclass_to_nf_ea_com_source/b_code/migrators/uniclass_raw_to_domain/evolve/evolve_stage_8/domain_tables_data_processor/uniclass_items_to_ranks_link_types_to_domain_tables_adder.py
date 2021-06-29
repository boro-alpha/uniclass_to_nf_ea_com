from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    LINKED_TABLE_UNICLASS_ITEMS_TO_RANKS, UNICLASS_CLASSIFICATION_TYPE_OF_RELATION, OBJECT_NAME_COLUMN_NAME, \
    UUID_COLUMN_NAME, RELATION_TYPE_UUIDS_COLUMN_NAME, RELATION_TYPE_NAMES_COLUMN_NAME


def add_uniclass_items_to_ranks_link_types_to_domain_tables(
        evolve_8_domain_tables: dict) \
        -> dict:
    uniclass_top_level_core_objects = \
        evolve_8_domain_tables[
            'uniclass_top_level_core_objects']

    linked_table_uniclass_items_to_ranks = \
        evolve_8_domain_tables[
            LINKED_TABLE_UNICLASS_ITEMS_TO_RANKS]

    linked_table_uniclass_items_to_ranks[
        RELATION_TYPE_NAMES_COLUMN_NAME] = \
        UNICLASS_CLASSIFICATION_TYPE_OF_RELATION

    linked_table_uniclass_items_to_ranks_with_link_type_uuids = \
        left_merge_dataframes(
            master_dataframe=linked_table_uniclass_items_to_ranks,
            master_dataframe_key_columns=[RELATION_TYPE_NAMES_COLUMN_NAME],
            merge_suffixes=['1', '2'],
            foreign_key_dataframe=uniclass_top_level_core_objects,
            foreign_key_dataframe_fk_columns=[OBJECT_NAME_COLUMN_NAME],
            foreign_key_dataframe_other_column_rename_dictionary={
                UUID_COLUMN_NAME: RELATION_TYPE_UUIDS_COLUMN_NAME})

    evolve_8_domain_tables[
        LINKED_TABLE_UNICLASS_ITEMS_TO_RANKS] = \
        linked_table_uniclass_items_to_ranks_with_link_type_uuids

    return \
        evolve_8_domain_tables
