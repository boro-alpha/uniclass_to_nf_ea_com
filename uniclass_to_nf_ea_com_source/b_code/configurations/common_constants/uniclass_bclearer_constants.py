from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_attribute_table_column_types import \
    StandardAttributeTableColumnTypes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_connector_table_column_types import \
    StandardConnectorTableColumnTypes
from nf_ea_common_tools_source.b_code.nf_ea_common.common_knowledge.column_types.nf_domains.standard_object_table_column_types import \
    StandardObjectTableColumnTypes

ROOT_OUTPUT_FOLDER_TITLE_MESSAGE = \
    'Please select root output folder'

UNICLASS_BCLEARER_PREFIX_FOR_NAMES = \
    'uniclass_bclearer_'

# Domain Tables Names

UNICLASS_LOAD_STAGE_4_DOMAIN_TABLES = \
    'load_4_domain_tables'
UNICLASS_EVOLVE_STAGE_1_DOMAIN_TABLES = \
    'evolve_1_domain_tables'
UNICLASS_EVOLVE_STAGE_2_DOMAIN_TABLES = \
    'evolve_2_domain_tables'
UNICLASS_EVOLVE_STAGE_3_DOMAIN_TABLES = \
    'evolve_3_domain_tables'
UNICLASS_EVOLVE_STAGE_4_DOMAIN_TABLES = \
    'evolve_4_domain_tables'
UNICLASS_EVOLVE_STAGE_5_DOMAIN_TABLES = \
    'evolve_5_domain_tables'
UNICLASS_EVOLVE_STAGE_6_DOMAIN_TABLES = \
    'evolve_6_domain_tables'
UNICLASS_EVOLVE_STAGE_7_DOMAIN_TABLES = \
    'evolve_7_domain_tables'
UNICLASS_EVOLVE_STAGE_8_DOMAIN_TABLES = \
    'evolve_8_domain_tables'

# LINKED TABLES EVOLVE 8 INPUT

ATTRIBUTE_TABLE_HIGHER_LEVELS_NAME_SPACES_TABLE_NAME = \
    'attribute_table_higher_levels_name_spaces'
LINKED_TABLE_BCLEARER_FOUNDATION_PACKAGE_ = \
    'linked_table_bclearer_foundation_package'
LINKED_TABLE_ISO_12006_PACKAGE_ = \
    'linked_table_iso_12006_package'
LINKED_TABLE_NAMESPACES_PACKAGE_ = \
    'linked_table_namespaces_package'
LINKED_TABLE_PARENT_CHILD_SUBTYPES_PACKAGE_ = \
    'linked_table_parent_child_subtypes_package'
LINKED_TABLE_RANKS_PACKAGE_ = \
    'linked_table_ranks_package'
LINKED_TABLE_TOP_LEVEL_CORE_PACKAGE_ = \
    'linked_table_top_level_core_package'
LINKED_TABLE_UNICLASS_ITEM_TO_OBJECT_ = \
    'linked_table_uniclass_item_to_object'

# Value Names

PACKAGE_NAME = \
    'Package'
UNICLASS_RANKS_NAME = \
    'UNICLASS Ranks'
UNICLASS_PACKAGE_NAME = \
    'UNICLASS Package'
UNICLASS_ITEM_NAME = \
    'UNICLASS Item'
TOP_ITEM_NAME = \
    'Top Item'
AREA_NAME = \
    'Area'
GROUP_NAME = \
    'Group'
SUB_GROUP_NAME = \
    'Sub group'
SECTION_NAME = \
    'Section'
OBJECT_NAME = \
    'Object'
DEPENDENCY_NAME = \
    'Dependency'
UNICLASS_CLASSIFICATION_TYPE_OF_RELATION = \
    'uniclass_classification_type-of relation'

# column names
UUID_COLUMN_NAME = \
    StandardObjectTableColumnTypes.NF_UUIDS.column_name
UUID1_COLUMN_NAME = \
    StandardObjectTableColumnTypes.NF_UUIDS.column_name + '1'
UUID2_COLUMN_NAME = \
    StandardObjectTableColumnTypes.NF_UUIDS.column_name + '2'
CODE_COLUMN_NAME = \
    'Code'
TITLE_COLUMN_NAME = \
    'Title'
AREA_COLUMN_NAME = \
    'Area'
GROUP_COLUMN_NAME = \
    'Group'
SUB_GROUP_COLUMN_NAME = \
    'Sub group'
SECTION_COLUMN_NAME = \
    'Section'
OBJECT_COLUMN_NAME = \
    'Object'
RANKS_COLUMN_NAME = \
    'ranks'
PARENT_CODE_COLUMN_NAME = \
    'parent_code'
PARENT_UUID_COLUMN_NAME = \
    'parent_UUID'
PARENT_TITLE_COLUMN_NAME = \
    'parent_title'
PARENT_RANK_NAME_COLUMN_NAME = \
    'parent_rank_name'
CHILD_CODE_COLUMN_NAME = \
    'child_code'
CHILD_RANK_NAME_COLUMN_NAME = \
    'child_rank_name'
CHILD_RANK_NAME1_COLUMN_NAME = \
    'child_rank_name1'
CHILD_RANK_NAME2_COLUMN_NAME = \
    'child_rank_name2'
CHILD_RANK_COLUMN_NAME = \
    'child_rank'
CHILD_UUID_COLUMN_NAME = \
    'child_UUID'
CHILD_TITLE_COLUMN_NAME = \
    'child_title'
RANK_NAME_COLUMN_NAME = \
    'rank_name'
LINK_TYPES_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.CONNECTOR_UML_TYPE_IDENTIFIERS.column_name
LINK_NAMES_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.CONNECTOR_UML_NAMES.column_name
PARENT_UUIDS_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.CLIENT_PLACE_2_NF_UUIDS.column_name
PARENT_UUIDS1_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.CLIENT_PLACE_2_NF_UUIDS.column_name + '1'
PARENT_UUIDS2_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.CLIENT_PLACE_2_NF_UUIDS.column_name + '2'
CHILD_UUIDS_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.SUPPLIER_PLACE_1_NF_UUIDS.column_name
CHILD_UUIDS1_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.SUPPLIER_PLACE_1_NF_UUIDS.column_name + '1'
CHILD_UUIDS2_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.SUPPLIER_PLACE_1_NF_UUIDS.column_name + '2'
CHILD_NAMES_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.SUPPLIER_PLACE_1_UML_NAMES.column_name
PARENT_NAMES_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.CLIENT_PLACE_2_UML_NAMES.column_name
STEREOTYPE_UUIDS_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.STEREOTYPE_NF_UUIDS.column_name
STEREOTYPE_NAMES_COLUMN_NAME = \
    StandardConnectorTableColumnTypes.STEREOTYPE_UML_NAMES.column_name
CHILD_PACKAGE_NAME_COLUMN_NAME = \
    'child_package_name'
PARENT_PACKAGE_NAME_COLUMN_NAME = \
    StandardObjectTableColumnTypes.PARENT_PACKAGE_UML_NAMES.column_name
OBJECT_NAME_COLUMN_NAME = \
    StandardObjectTableColumnTypes.UML_OBJECT_NAMES.column_name

# NF COM TABLES COLUMN NAMES

PARENT_PACKAGE_UUID_COLUMN_NAME = \
    StandardObjectTableColumnTypes.PARENT_PACKAGE_NF_UUIDS.column_name
PARENT_PACKAGE_UUID1_COLUMN_NAME = \
    StandardObjectTableColumnTypes.PARENT_PACKAGE_NF_UUIDS.column_name + '1'
PARENT_PACKAGE_UUID2_COLUMN_NAME = \
    StandardObjectTableColumnTypes.PARENT_PACKAGE_NF_UUIDS.column_name + '2'

# table names

TOP_ITEMS_TABLE_NAME = \
    "Uniclass2015_top_level_objects_UTF8_OXi"
UNICLASS2015_OBJECT_TABLE_NAME = \
    'uniclass2015_object_table'
UNICLASS_PARENT_CHILD_LINK_TABLE_NAME = \
    'uniclass2015_parent_child_link_table'
UNICLASS_STEREOTYPES_TYPE_OF_TABLE_NAME = \
    'uniclass_stereotypes_type_of_table'
UNICLASS2015_RANKS_TABLE_NAME = \
    'uniclass2015_ranks_table'
UNICLASS2015_TOP_LEVEL_OBJECTS_TABLE_NAME = \
    "Uniclass2015_top_level_objects_UTF8_OXi"
LINKED_TABLE_UNICLASS_ITEMS_TO_RANKS = \
    'linked_table_uniclass_items_to_ranks'

# file extension names

CSV_EXTENSION_FILE_NAME = \
    ".csv"

# STAGE OUTPUT FILE NAME
EVOLVE_STAGE_3_OUTPUT_FILE_NAME = \
    'uniclass2015_concatenated_data'

# nf com table names

BCLEARER_FOUNDATION_OBJECTS_TABLE_NAME = \
    'bclearer_foundation_objects'
CLASSIFICATION_RANKS_NAMES_TABLE_NAME = \
    'classification_ranks_names'
ISO_12006_OBJECTS_TABLE_NAME = \
    'iso_12006_objects'
PARENT_CHILD_SUBTYPES_TABLE_NAME = \
    'parent_child_subtypes'
UNICLASS_NAMING_SPACES_OBJECTS_TABLE_NAME = \
    'uniclass_naming_spaces_objects'
UNICLASS_TOP_LEVEL_CORE_OBJECTS_TABLE_NAME = \
    'uniclass_top_level_core_objects'
UUIDIFIED_PACKAGES_TABLE_NAME = \
    'uuidified_packages'

# UUIDIFIED PACKAGES TABLE COLUMN NAMES

EA_GUIDS_COLUMN_NAME = \
    'ea_guids'
EA_PACKAGE_PATH_COLUMN_NAME = \
    'ea_package_path'
PARENT_PACKAGE_EA_GUID_COLUMN_NAME = \
    'parent_package_ea_guid'
NF_UUIDS_COLUMN_NAME = \
    'nf_uuids'
CONTAINED_EA_PACKAGES_COLUMN_NAME = \
    'contained_ea_packages'
EA_OBJECT_TYPE_COLUMN_NAME = \
    'ea_object_type'
SUPPLIER_PLACE1_END_CONNECTORS_COLUMN_NAME = \
    'supplier_place1_end_connectors'
CLIENT_PLACE2_END_CONNECTORS_COLUMN_NAME = \
    'client_place2_end_connectors'
CONTAINED_EA_DIAGRAMS_COLUMN_NAME = \
    'contained_ea_diagrams'
CONTAINED_EA_CLASSIFIERS_COLUMN_NAME = \
    'contained_ea_classifiers'
PARENT_EA_ELEMENT_COLUMN_NAME = \
    'parent_ea_element'
PARENT_EA_ELEMENT1_COLUMN_NAME = \
    'parent_ea_element1'
PARENT_EA_ELEMENT_NAME_COLUMN_NAME = \
    'parent_ea_element_name'
EA_OBJECT_STEREOTYPES_COLUMN_NAME = \
    'ea_object_stereotypes'
EA_REPOSITORY_COLUMN_NAME = \
    'ea_repository'
EA_OBJECT_NAME_COLUMN_NAME = \
    'ea_object_name'
EA_OBJECT_NOTES_COLUMN_NAME = \
    'ea_object_notes'
EA_GUID_COLUMN_NAME = \
    'ea_guid'

# BCLEARER STAGES

LOAD_STAGE_4_NAME = \
    'load_4'
EVOLVE_STAGE_1_NAME = \
    'evolve_1'
EVOLVE_STAGE_2_NAME = \
    'evolve_2'
EVOLVE_STAGE_3_NAME = \
    'evolve_3'
EVOLVE_STAGE_4_NAME = \
    'evolve_4'
EVOLVE_STAGE_5_NAME = \
    'evolve_5'
EVOLVE_STAGE_6_NAME = \
    'evolve_6'
EVOLVE_STAGE_7_NAME = \
    'evolve_7'
EVOLVE_STAGE_8_NAME = \
    'evolve_8'

# Attributes table column names

ATTRIBUTED_OBJECT_UUIDS_COLUMN_NAME = \
    StandardAttributeTableColumnTypes.ATTRIBUTED_OBJECT_UUIDS.column_name
ATTRIBUTED_OBJECT_NAMES_COLUMN_NAME = \
    'attributed_object_names'
ATTRIBUTED_OBJECT_PACKAGE_NAMES_COLUMN_NAME = \
    'attributed_object_package_names'
ATTRIBUTE_TYPE_UUIDS_COLUMN_NAME = \
    StandardAttributeTableColumnTypes.ATTRIBUTE_TYPE_UUIDS.column_name
ATTRIBUTE_TYPE_NAMES_COLUMN_NAME = \
    'attribute_type_names'
ATTRIBUTE_TYPE_PACKAGE_NAMES_COLUMN_NAME = \
    'attribute_type_package_names'
UML_VISIBILITY_KIND_COLUMN_NAME = \
    StandardAttributeTableColumnTypes.UML_VISIBILITY_KIND.column_name
ATTRIBUTE_VALUES_COLUMN_NAME = \
    StandardAttributeTableColumnTypes.ATTRIBUTE_VALUES.column_name
RELATION_TYPE_NAMES_COLUMN_NAME = \
    'type_names'
RELATION_TYPE_UUIDS_COLUMN_NAME = \
    'type_uuids'
