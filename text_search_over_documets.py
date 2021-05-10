from text_search_over_documets_abc_meta import TextSearchDocumentABCMeta
from elasticsearch import Elasticsearch
from enum import Enum
import math


class TextSearchDocuments(TextSearchDocumentABCMeta):

    # TODO
    class _Index(Enum):
        pass

    def __init__(self, connect_url, doc_type="doc"):
        self._es = Elasticsearch(connect_url)
        self._doc_type = doc_type

    def _make_query(self, string_look_for):
        look_for = "*{}*".format(string_look_for.lower())
        query_body = {
            "query": {
                "wildcard": {
                    'text': look_for
                }
            }
        }
        return query_body

    def _get_index_from_object_type(self, object_type):
        return "{}_index".format(object_type)

    def _get_keys_from_search_results(self, search_result_list):
        keys_result_list = []
        for search_result in search_result_list['hits']['hits']:
            keys_result_list.append(search_result['_source']['key'])
        return keys_result_list

    def check_for_text(self, object_type, string_look_for, max_list_size=10):
        if string_look_for == "": #TODO is that correct? return all?
            return []
        query = self._make_query(string_look_for=string_look_for)
        search_index = self._get_index_from_object_type(object_type)
        search_result_list = self._es.search(index=search_index, doc_type=self._doc_type, body=query, size=max_list_size)
        keys_result_list = self._get_keys_from_search_results(search_result_list=search_result_list)
        return keys_result_list

    def disconnect(self):
        self._es.transport.close()
