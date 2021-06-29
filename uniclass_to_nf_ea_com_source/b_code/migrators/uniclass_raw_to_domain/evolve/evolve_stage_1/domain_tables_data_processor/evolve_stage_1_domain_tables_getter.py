from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_1.domain_tables_data_processor.top_level_items_rows_adder import \
    add_top_level_item_rows_to_dictionary_of_dataframes
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_1.domain_tables_data_processor.top_level_items_table_remover import \
    remove_top_level_items_table_from_dataframes_dictionary


def get_evolve_stage_1_domain_tables(
        dictionary_of_dataframes: dict) \
        -> dict:

    uuidified_dataframe_dictionary_with_top_level_table = \
        add_top_level_item_rows_to_dictionary_of_dataframes(
            dictionary_of_dataframes=dictionary_of_dataframes)

    top_level_dataframe_dictionary = \
        remove_top_level_items_table_from_dataframes_dictionary(
            dataframe_dictionary=uuidified_dataframe_dictionary_with_top_level_table)

    return \
        top_level_dataframe_dictionary
