from graphene import String
import graphene

from text_search_over_documets import TextSearchDocuments

class GraphQLSearch(graphene.ObjectType):
    text_look = String(look=graphene.String())
    def resolve_text_look(self,info, look):
        txt_search_documents_val = TextSearchDocuments(connect_url='http://localhost:9200/')
        res = txt_search_documents_val.check_for_text(object_type="items",
                                                      string_look_for=look, max_list_size=1000)
        txt_search_documents_val.disconnect()
        return res

text_search_schema = graphene.Schema(query=GraphQLSearch, mutation=None, auto_camelcase=False)
