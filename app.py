
from flask import Flask
from flask_graphql import GraphQLView
from schema import text_search_schema
from flask_graphql import GraphQLView

from text_index_documents_threads import TextIndexDocuments
import json

app = Flask(__name__)

app.add_url_rule('/text_search', view_func=GraphQLView.as_view('content_protection_explorer', schema=text_search_schema, graphiql=True))

def update_es_real_json():
    with open('data.json') as f:
        data = json.load(f)
        list_docs = data['data']['entities']['items']
        input_key = 'id'
        input_list_fields = ['description', 'keywords.text', 'keywords.tags.display_name']
        input_object_type = "items"
        txt_index_documents_val = TextIndexDocuments(connect_url='http://localhost:9200/')

        txt_index_documents_val.add_documents(docs_to_check_list=list_docs,
                                              search_field_list=input_list_fields,
                                              key=input_key,
                                              object_type=input_object_type)

        txt_index_documents_val.disconnect()

if __name__ == '__main__':
    update_es_real_json()
    #
    app.run(host='0.0.0.0',port=4500)