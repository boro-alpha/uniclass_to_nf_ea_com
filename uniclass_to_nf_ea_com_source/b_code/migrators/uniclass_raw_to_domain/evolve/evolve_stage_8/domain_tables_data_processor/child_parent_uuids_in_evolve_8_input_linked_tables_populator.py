import pandas
from nf_common_source.code.services.dataframe_service.dataframe_helpers.dataframe_filter_and_renamer import \
    dataframe_filter_and_rename
from nf_common_source.code.services.dataframe_service.dataframe_mergers import left_merge_dataframes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_attribute_table_column_types import \
    StandardAttributeTableColumnTypes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_connector_table_column_types import \
    StandardConnectorTableColumnTypes

from uniclass_to_nf_ea_com_source.b_code.configurations.common_constants.uniclass_bclearer_constants import \
    PARENT_PACKAGE_NAME_COLUMN_NAME, \
    UUID_COLUMN_NAME, CHILD_UUIDS_COLUMN_NAME, \
    ATTRIBUTE_TABLE_HIGHER_LEVELS_NAME_SPACES_TABLE_NAME, CHILD_NAMES_COLUMN_NAME, \
    PARENT_UUIDS_COLUMN_NAME, PARENT_NAMES_COLUMN_NAME, \
    LINK_TYPES_COLUMN_NAME, LINK_NAMES_COLUMN_NAME, STEREOTYPE_UUIDS_COLUMN_NAME, STEREOTYPE_NAMES_COLUMN_NAME, \
    CHILD_PACKAGE_NAME_COLUMN_NAME, OBJECT_NAME_COLUMN_NAME, ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME, \
    ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME, ATTRIBUTE_TYPE_UUIDS_COLUMN_NAME, ATTRIBUTE_TYPE_NAMES_COLUMN_NAME, \
    ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME, UML_VISIBILITY_KIND_COLUMN_NAME, ATTRIBUTE_VALUES_COLUMN_NAME, \
    ATTRIBUTED_OBJECT_UUIDS_COLUMN_NAME


def populate_child_parent_uuids_in_evolve_8_input_linked_tables(
        evolve_8_input_linked_tables: dict,
        evolve_8_domain_tables: dict) \
        -> dict:
    attribute_table_higher_levels_name_spaces = \
        evolve_8_input_linked_tables.pop(
            ATTRIBUTE_TABLE_HIGHER_LEVELS_NAME_SPACES_TABLE_NAME)

    evolve_8_input_linked_tables_with_child_uuids_populated = \
        __populate_child_uuid_columns_in_linked_tables(
            evolve_8_input_linked_tables=evolve_8_input_linked_tables,
            evolve_8_domain_tables=evolve_8_domain_tables)

    evolve_8_input_linked_tables_with_child_parent_uuids_populated = \
        __populate_parent_uuid_columns_in_linked_tables(
            evolve_8_input_linked_tables=evolve_8_input_linked_tables_with_child_uuids_populated,
            evolve_8_domain_tables=evolve_8_domain_tables)

    attribute_table_higher_levels_name_spaces_with_attributed_object_uuids = \
        __add_attributed_object_uuids_to_table(
            attribute_table_higher_levels_name_spaces=attribute_table_higher_levels_name_spaces,
            evolve_8_object_tables=evolve_8_domain_tables)

    attribute_table_higher_levels_name_spaces_with_attributed_type_uuids = \
        __add_attributed_type_uuids_to_table(
            attribute_table_higher_levels_name_spaces=attribute_table_higher_levels_name_spaces_with_attributed_object_uuids,
            evolve_8_object_tables=evolve_8_domain_tables)

    evolve_8_input_linked_tables_with_child_parent_uuids_populated[
        ATTRIBUTE_TABLE_HIGHER_LEVELS_NAME_SPACES_TABLE_NAME] = \
        attribute_table_higher_levels_name_spaces_with_attributed_type_uuids

    return \
        evolve_8_input_linked_tables_with_child_parent_uuids_populated


def __populate_child_uuid_columns_in_linked_tables(
        evolve_8_input_linked_tables: dict,
        evolve_8_domain_tables: dict) \
        -> dict:
    evolve_8_input_linked_tables_with_child_uuids_populated = \
        {}

    populated_dataframe_columns = {
        UUID_COLUMN_NAME: UUID_COLUMN_NAME,
        'temporary_nf_uuid': CHILD_UUIDS_COLUMN_NAME,
        CHILD_NAMES_COLUMN_NAME: CHILD_NAMES_COLUMN_NAME,
        PARENT_UUIDS_COLUMN_NAME: PARENT_UUIDS_COLUMN_NAME,
        PARENT_NAMES_COLUMN_NAME: PARENT_NAMES_COLUMN_NAME,
        LINK_TYPES_COLUMN_NAME: LINK_TYPES_COLUMN_NAME,
        LINK_NAMES_COLUMN_NAME: LINK_NAMES_COLUMN_NAME,
        STEREOTYPE_UUIDS_COLUMN_NAME: STEREOTYPE_UUIDS_COLUMN_NAME,
        STEREOTYPE_NAMES_COLUMN_NAME: STEREOTYPE_NAMES_COLUMN_NAME,
        CHILD_PACKAGE_NAME_COLUMN_NAME: CHILD_PACKAGE_NAME_COLUMN_NAME,
        PARENT_PACKAGE_NAME_COLUMN_NAME: PARENT_PACKAGE_NAME_COLUMN_NAME
    }

    for linked_table_name, linked_table in evolve_8_input_linked_tables.items():
        if PARENT_PACKAGE_NAME_COLUMN_NAME in linked_table:
            linked_table_dataframe_with_child_uuids_values = \
                __add_uuids_to_linked_table_columns(
                    linked_table=linked_table,
                    evolve_8_domain_tables=evolve_8_domain_tables,
                    linked_table_column_name=StandardConnectorTableColumnTypes.SUPPLIER_PLACE_1_NF_UUIDS.column_name,
                    master_dataframe_key_columns=[CHILD_NAMES_COLUMN_NAME, CHILD_PACKAGE_NAME_COLUMN_NAME],
                    foreign_key_dataframe_fk_columns=[OBJECT_NAME_COLUMN_NAME, PARENT_PACKAGE_NAME_COLUMN_NAME],
                    column_renaming_dictionary=populated_dataframe_columns)

            evolve_8_input_linked_tables_with_child_uuids_populated[linked_table_name] = \
                linked_table_dataframe_with_child_uuids_values

    return \
        evolve_8_input_linked_tables_with_child_uuids_populated


def __populate_parent_uuid_columns_in_linked_tables(
        evolve_8_input_linked_tables: dict,
        evolve_8_domain_tables: dict) \
        -> dict:
    evolve_8_input_linked_tables_with_parent_uuids_populated = \
        {}

    populated_dataframe_columns = {
        UUID_COLUMN_NAME: UUID_COLUMN_NAME,
        CHILD_UUIDS_COLUMN_NAME: CHILD_UUIDS_COLUMN_NAME,
        CHILD_NAMES_COLUMN_NAME: CHILD_NAMES_COLUMN_NAME,
        'temporary_nf_uuid': PARENT_UUIDS_COLUMN_NAME,
        PARENT_NAMES_COLUMN_NAME: PARENT_NAMES_COLUMN_NAME,
        LINK_TYPES_COLUMN_NAME: LINK_TYPES_COLUMN_NAME,
        LINK_NAMES_COLUMN_NAME: LINK_NAMES_COLUMN_NAME,
        STEREOTYPE_UUIDS_COLUMN_NAME: STEREOTYPE_UUIDS_COLUMN_NAME,
        STEREOTYPE_NAMES_COLUMN_NAME: STEREOTYPE_NAMES_COLUMN_NAME,
        CHILD_PACKAGE_NAME_COLUMN_NAME: CHILD_PACKAGE_NAME_COLUMN_NAME,
        PARENT_PACKAGE_NAME_COLUMN_NAME: PARENT_PACKAGE_NAME_COLUMN_NAME
    }

    for table_name_linked_table, dataframe_linked_table in evolve_8_input_linked_tables.items():
        if PARENT_PACKAGE_NAME_COLUMN_NAME in dataframe_linked_table:
            linked_table_dataframe_with_parent_uuids_values = \
                __add_uuids_to_linked_table_columns(
                    linked_table=dataframe_linked_table,
                    evolve_8_domain_tables=evolve_8_domain_tables,
                    linked_table_column_name=StandardConnectorTableColumnTypes.CLIENT_PLACE_2_NF_UUIDS.column_name,
                    master_dataframe_key_columns=[PARENT_NAMES_COLUMN_NAME, PARENT_PACKAGE_NAME_COLUMN_NAME],
                    foreign_key_dataframe_fk_columns=[OBJECT_NAME_COLUMN_NAME, PARENT_PACKAGE_NAME_COLUMN_NAME],
                    column_renaming_dictionary=populated_dataframe_columns)

            evolve_8_input_linked_tables_with_parent_uuids_populated[table_name_linked_table] = \
                linked_table_dataframe_with_parent_uuids_values

    return \
        evolve_8_input_linked_tables_with_parent_uuids_populated


def __add_attributed_object_uuids_to_table(
        attribute_table_higher_levels_name_spaces: pandas.DataFrame,
        evolve_8_object_tables: dict) -> pandas.DataFrame:

    populated_dataframe_columns = \
        {
            UUID_COLUMN_NAME: UUID_COLUMN_NAME,
            'temporary_nf_uuid':ATTRIBUTED_OBJECT_UUIDS_COLUMN_NAME,
            ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME: ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME,
            ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME: ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME,
            ATTRIBUTE_TYPE_UUIDS_COLUMN_NAME: ATTRIBUTE_TYPE_UUIDS_COLUMN_NAME,
            ATTRIBUTE_TYPE_NAMES_COLUMN_NAME: ATTRIBUTE_TYPE_NAMES_COLUMN_NAME,
            ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME: ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME,
            UML_VISIBILITY_KIND_COLUMN_NAME: UML_VISIBILITY_KIND_COLUMN_NAME,
            ATTRIBUTE_VALUES_COLUMN_NAME: ATTRIBUTE_VALUES_COLUMN_NAME

        }

    table_with_attributed_object_uuids = \
        __add_uuids_to_linked_table_columns(
            linked_table=attribute_table_higher_levels_name_spaces,
            evolve_8_domain_tables=evolve_8_object_tables,
            linked_table_column_name=StandardAttributeTableColumnTypes.ATTRIBUTED_OBJECT_UUIDS.column_name,
            master_dataframe_key_columns=[ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME,
                                          ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME],
            foreign_key_dataframe_fk_columns=[OBJECT_NAME_COLUMN_NAME, PARENT_PACKAGE_NAME_COLUMN_NAME],
            column_renaming_dictionary=populated_dataframe_columns)

    return \
        table_with_attributed_object_uuids


def __add_attributed_type_uuids_to_table(
        attribute_table_higher_levels_name_spaces: pandas.DataFrame,
        evolve_8_object_tables: dict) -> pandas.DataFrame:

    populated_dataframe_columns = \
        {
            UUID_COLUMN_NAME: UUID_COLUMN_NAME,
            ATTRIBUTED_OBJECT_UUIDS_COLUMN_NAME:ATTRIBUTED_OBJECT_UUIDS_COLUMN_NAME,
            ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME: ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME,
            ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME: ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME,
            'temporary_nf_uuid': ATTRIBUTE_TYPE_UUIDS_COLUMN_NAME,
            ATTRIBUTE_TYPE_NAMES_COLUMN_NAME: ATTRIBUTE_TYPE_NAMES_COLUMN_NAME,
            ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME: ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME,
            UML_VISIBILITY_KIND_COLUMN_NAME: UML_VISIBILITY_KIND_COLUMN_NAME,
            ATTRIBUTE_VALUES_COLUMN_NAME: ATTRIBUTE_VALUES_COLUMN_NAME

        }

    table_with_attributed_type_uuids = \
        __add_uuids_to_linked_table_columns(
            linked_table=attribute_table_higher_levels_name_spaces,
            evolve_8_domain_tables=evolve_8_object_tables,
            linked_table_column_name=StandardAttributeTableColumnTypes.ATTRIBUTE_TYPE_UUIDS.column_name,
            master_dataframe_key_columns=[ATTRIBUTE_TYPE_NAMES_COLUMN_NAME,
                                          ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME],
            foreign_key_dataframe_fk_columns=[OBJECT_NAME_COLUMN_NAME, PARENT_PACKAGE_NAME_COLUMN_NAME],
            column_renaming_dictionary=populated_dataframe_columns)

    return \
        table_with_attributed_type_uuids


def __add_uuids_to_linked_table_columns(
        linked_table: pandas.DataFrame,
        evolve_8_domain_tables: dict,
        linked_table_column_name: str,
        master_dataframe_key_columns: list,
        foreign_key_dataframe_fk_columns: list,
        column_renaming_dictionary: dict) \
        -> pandas.DataFrame:
    linked_table_uuids_unpopulated_slice = \
        linked_table[linked_table[
            linked_table_column_name].isnull()]

    linked_table_uuids_populated_slice = \
        linked_table[linked_table[
            linked_table_column_name].notnull()]

    for evolve_8_domain_table_name, evolve_8_domain_table in evolve_8_domain_tables.items():
        if OBJECT_NAME_COLUMN_NAME in evolve_8_domain_table.columns:
            if not linked_table_uuids_unpopulated_slice.empty:
                temporary_dataframe_with_child_uuids_populated = \
                    left_merge_dataframes(
                        master_dataframe=linked_table_uuids_unpopulated_slice,
                        master_dataframe_key_columns=master_dataframe_key_columns,
                        merge_suffixes=['1', '2'],
                        foreign_key_dataframe=evolve_8_domain_table,
                        foreign_key_dataframe_fk_columns=foreign_key_dataframe_fk_columns,
                        foreign_key_dataframe_other_column_rename_dictionary={
                            UUID_COLUMN_NAME: 'temporary_nf_uuid'})

                dataframe_with_child_uuid_column_populated_dictionary = \
                    column_renaming_dictionary

                temporary_dataframe_with_child_uuids_populated = \
                    dataframe_filter_and_rename(
                        dataframe=temporary_dataframe_with_child_uuids_populated,
                        filter_and_rename_dictionary=dataframe_with_child_uuid_column_populated_dictionary)

                linked_table_uuids_unpopulated_slice = \
                    temporary_dataframe_with_child_uuids_populated[
                        temporary_dataframe_with_child_uuids_populated[
                            linked_table_column_name].isnull()]

                linked_table_uuids_populated_slice = \
                    pandas.concat([
                        linked_table_uuids_populated_slice,
                        temporary_dataframe_with_child_uuids_populated[
                            temporary_dataframe_with_child_uuids_populated[
                                linked_table_column_name].notnull()]],
                        ignore_index=True)

    linked_table_uuids_populated_slice = \
        pandas.concat(
            [linked_table_uuids_populated_slice,
             linked_table_uuids_unpopulated_slice
             ], ignore_index=True
        )

    return \
        linked_table_uuids_populated_slice
