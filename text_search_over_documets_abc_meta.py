import abc


class TextSearchDocumentABCMeta(object):
    __metaclass__ = abc.ABCMeta
    @abc.abstractmethod
    def check_for_text(self, object_type, string_look_for, max_list_size):
        raise NotImplementedError
