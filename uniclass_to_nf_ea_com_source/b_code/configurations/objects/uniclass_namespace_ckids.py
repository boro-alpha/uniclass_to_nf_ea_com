from enum import auto
from enum import unique

from nf_common_source.code.nf.types.collection_types import CollectionTypes


@unique
class UniclassNamespaceCkIds(
        CollectionTypes):
    AREA = auto()
    CODE = auto()
    GROUP = auto()
    SUB_GROUP = auto()
    SECTION = auto()
    OBJECT = auto()
    TITLE = auto()
    UNICLASS_DOMAIN_NAMES = auto()
    UNICLASS_CLASSIFICATION_RANKS_NAMES = auto()
    UNICLASS_CLASSIFICATION_TYPE_OF_RELATION_SUB_TYPE_NAMES = auto()
    RANK_TYPE_NAME = auto()

    def __collection_name(
            self) \
            -> str:
        return \
            self.to_string()

    collection_name = \
        property(
            fget=__collection_name)

    def to_string(
            self)\
            -> str:
        string = \
            name_mapping[self]

        return \
            string


name_mapping = \
    {
        UniclassNamespaceCkIds.AREA: 'Area',
        UniclassNamespaceCkIds.CODE: 'Code',
        UniclassNamespaceCkIds.GROUP: 'Group',
        UniclassNamespaceCkIds.SUB_GROUP: 'Sub group',
        UniclassNamespaceCkIds.SECTION: 'Section',
        UniclassNamespaceCkIds.OBJECT: 'Object',
        UniclassNamespaceCkIds.TITLE: 'Title',
        UniclassNamespaceCkIds.UNICLASS_DOMAIN_NAMES: 'UNICLASS Domain Names',
        UniclassNamespaceCkIds.UNICLASS_CLASSIFICATION_RANKS_NAMES: 'UNICLASS Classification Ranks Names',
        UniclassNamespaceCkIds.UNICLASS_CLASSIFICATION_TYPE_OF_RELATION_SUB_TYPE_NAMES: 'uniclass_classification-type-of relation sub-type names',
        UniclassNamespaceCkIds.RANK_TYPE_NAME: 'rank_type_Name'
    }
