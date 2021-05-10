import json
import unittest
from datetime import datetime
from time import time
from elasticsearch import Elasticsearch

from text_index_documents_threads import TextIndexDocuments
from text_search_over_documets import TextSearchDocuments


class MyTestCase(unittest.TestCase):

    def _clean_table(self, object_type, es, doc_type):
        es.index(index="{}_index".format(object_type), doc_type=doc_type, id=1, body={})
        es.indices.refresh()
        res = es.search(
            index="{}_index".format(object_type),
            doc_type=self.doc_type,
            body={"query": {"match_all": {}}},
            size=2000)
        ids = [d['_id'] for d in res['hits']['hits']]
        for i in ids:
            es.delete(index="{}_index".format(object_type), doc_type=doc_type, id=i)
        es.indices.refresh()

    def setUp(self):
        es = Elasticsearch(connect_url='http://localhost:9200/')
        self.doc_type = "doc"
        self.input_object_type = "object_type_name"
        self.object_type_json = "object_type_real_json"
        self._clean_table(self.input_object_type, es, self.doc_type)
        self._clean_table(self.object_type_json, es, self.doc_type)

        es.transport.close()

        self.txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')
        self.txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')




    def test_indexing_and_search_small_amount(self):
        amount_in = 10
        list_docs = []
        for i in range(0, amount_in):
            list_docs.append({
                'id': i,
                'author': 'kimchy',
                'title1': 'heeey: gogigaa. bonsai cool.',
                'title2': 'Elasticsearch: cool. bonsai c.',
                'timestamp': datetime.now(),
                'nested1': {
                    'nested2': 'eaeeea',
                },
                'nested_list': [
                    {
                        'nested2': 'dogi',
                    },
                    {
                        'nested2': 'eaeeea',
                    }
                ],
                'desc': 'blabla'
            })
        input_key = 'id'
        input_look_for = 'dogi'
        input_list_fields = ['title1', 'title2', 'nested1.nested2', 'nested_list.nested2']

        index_start_time = time()
        self.txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                              search_field_list=input_list_fields,
                                              key=input_key,
                                              object_type=self.input_object_type)
        print("indexing time {}: {}".format(amount_in,time() - index_start_time))
        search_start_time = time()
        res = self.txt_search_documents_val.check_for_text(object_type=self.input_object_type,
                                                      string_look_for=input_look_for, max_list_size=1000)
        print("searching time {}: {}".format(amount_in,time() - search_start_time))
        res.sort()
        self.assertTrue(res == [i for i in range(0, amount_in)])

    def test_indexing_and_search_big_amount(self):
        amount_in = 1000
        list_docs = []
        for i in range(0, amount_in):
            list_docs.append({
                'id': i,
                'author': 'kimchy',
                'title1': 'heeey: gogigaa. bonsai cool.',
                'title2': 'Elasticsearch: cool. bonsai c.',
                'timestamp': datetime.now(),
                'nested1': {
                    'nested2': 'eaeeea',
                },
                'desc': 'blabla'
            })
        input_key = 'id'
        input_look_for = 'eaeeea'
        input_list_fields = ['title1', 'title2', 'nested1.nested2']

        index_start_time = time()
        self.txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                              search_field_list=input_list_fields,
                                              key=input_key,
                                              object_type=self.input_object_type)
        print("indexing time {}: {}".format(amount_in, time() - index_start_time))
        search_start_time = time()
        res = self.txt_search_documents_val.check_for_text(object_type=self.input_object_type,
                                                      string_look_for=input_look_for, max_list_size=1000)
        print("searching time {}: {}".format(amount_in, time() - search_start_time))
        res.sort()
        self.assertTrue(res == [i for i in range(0, amount_in)])


    def test_empty(self):
        es = Elasticsearch(connect_url='http://localhost:9200/')
        query_body = {
            "query": {
                "match_all": {}
            }
        }
        search_result_list = es.search(index="{}_index".format(self.input_object_type),
                                       doc_type= self.doc_type,
                                       body=query_body,
                                       size=1000)
        self.assertEqual(0, search_result_list['hits']['total'])
        es.transport.close()

    def test_real_json(self):
        with open('data.json') as f:
            data = json.load(f)
            list_docs = data['data']['entities']['items']
            input_key = 'id'
            input_look_for = 'NBA'
            input_list_fields = ['description', 'keywords.text', 'keywords.tags.display_name']
            index_start_time = time()

            self.txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                                       search_field_list=input_list_fields,
                                                       key=input_key,
                                                       object_type=self.object_type_json)
            print("indexing time real json: {}".format(time() - index_start_time))
            search_start_time = time()
            res = self.txt_search_documents_val.check_for_text(object_type=self.object_type_json,
                                                                string_look_for=input_look_for, max_list_size=1000)
            print("searching time real json: {}".format(time() - search_start_time))
            print("Result len: {}, list: \n{}".format(len(res),res))
            #self.assertTrue(collections.Counter(res) == collections.Counter([i for i in range(0, amount_in)]))




    def tearDown(self):
        self.txt_index_documents_val.disconnect()
        self.txt_search_documents_val.disconnect()


if __name__ == '__main__':
    unittest.main()