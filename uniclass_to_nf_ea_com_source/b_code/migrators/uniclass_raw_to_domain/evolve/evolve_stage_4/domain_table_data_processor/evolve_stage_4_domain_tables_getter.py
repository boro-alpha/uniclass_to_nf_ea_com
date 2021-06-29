from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.dictionary_of_dataframes_generator import add_stereotypes_table_to_dataframes_dictionary
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.parent_child_link_table_creator import create_parent_child_link_table
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.parent_code_column_deprecator import deprecate_parent_code_column_from_uniclass_object_table
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.parent_code_column_adder import add_parent_code_column_to_uniclass_objects_table
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.parent_code_column_populator import populate_parent_code_column_in_uniclass_object_table
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.stereotypes_table_creator import create_stereotypes_table
from uniclass_to_nf_ea_com_source.b_code.migrators.uniclass_raw_to_domain.evolve.evolve_stage_4.domain_table_data_processor.redundant_attributes_remover import remove_redundant_attributes_from_object_table


def get_evolve_stage_4_domain_tables(
        dictionary_of_dataframes: dict) \
        -> dict:
    evolve_stage_4_dictionary_with_object_table_parent_code_column_added = \
        add_parent_code_column_to_uniclass_objects_table(
            dictionary_of_dataframes=dictionary_of_dataframes)

    evolve_stage_4_dictionary_with_object_table_parent_code_column_populated = \
        populate_parent_code_column_in_uniclass_object_table(
            dictionary_of_dataframes=evolve_stage_4_dictionary_with_object_table_parent_code_column_added)

    evolve_stage_4_dictionary_with_parent_child_link_table_added = \
        create_parent_child_link_table(
            dictionary_of_dataframes=evolve_stage_4_dictionary_with_object_table_parent_code_column_populated)

    evolve_stage_4_dictionary_with_object_table_redundant_attributes_removed = \
        remove_redundant_attributes_from_object_table(
            dictionary_of_dataframes=evolve_stage_4_dictionary_with_parent_child_link_table_added)

    uniclass_stereotypes_table = \
        create_stereotypes_table()

    evolve_stage_4_dictionary_with_object_table_parent_code_column_deprecated = \
        deprecate_parent_code_column_from_uniclass_object_table(
            dictionary_of_dataframes=evolve_stage_4_dictionary_with_object_table_redundant_attributes_removed)

    evolve_stage_4_domain_tables = \
        add_stereotypes_table_to_dataframes_dictionary(
            dictionary_of_dataframes=evolve_stage_4_dictionary_with_object_table_parent_code_column_deprecated,
            stereotypes_table=uniclass_stereotypes_table)

    return \
        evolve_stage_4_domain_tables
