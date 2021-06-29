def update_evolve_stage_8_domain_tables_with_input_linked_tables(
        evolve_8_domain_tables: dict,
        evolve_8_input_linked_tables: dict) \
        -> dict:
    evolve_8_domain_tables.update(
        evolve_8_input_linked_tables)

    return \
        evolve_8_domain_tables
