import abc


class TextIndexDocumentsABCMeta(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def add_documents(self, docs_to_check_list, search_field_list, key, object_type):
        raise NotImplementedError
