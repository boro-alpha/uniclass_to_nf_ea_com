from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_2.domain_tables_data_processor.area_column_adder import \
    add_area_column
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_2.domain_tables_data_processor.area_column_values_populator import \
    populate_area_column


def get_evolve_stage_2_domain_tables(
        dictionary_of_dataframes:dict)\
        -> dict:
    dataframe_dictionary_area_column = \
        add_area_column(
            dictionary_of_dataframes=dictionary_of_dataframes)

    dataframe_dictionary_area_column_values = \
        populate_area_column(
            dictionary_of_dataframes=dataframe_dictionary_area_column)

    return \
        dataframe_dictionary_area_column_values
