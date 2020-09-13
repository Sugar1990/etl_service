# -*- coding:utf-8 -*-
# author: Scandium
# work_location: Bei Jing
# File : es_service.py
# time: 2020/6/22 1:39
import math

from elasticsearch import Elasticsearch
from elasticsearch import helpers

from conf import ES_SERVER_IP, ES_SERVER_PORT


class ESService(object):
    def __init__(self, host, port=9200, username=None, password=None):
        if not username or not password:
            self.es = Elasticsearch(hosts=[host], port=port)
        else:
            self.es = Elasticsearch(hosts=[host], port=port, http_auth=(username, password))

    def creat_mapping_build(self, input_dic):
        if input_dic:
            default_dic = {
                "time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||date_optional_time"
                },
                "ik_keyword": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    },
                    "analyzer": "ik_smart"
                },
                "keyword": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },

                "ik": {
                    "type": "text",
                    "analyzer": "ik_smart"

                },

                "location": {
                    "type": "geo_point"
                },

                "id": {
                    "type": "long"
                },
                "json":{
                    "type": "nested"
                },
                "object":{
                    "type": "object"
                }
            }
            property = {}
            for key in input_dic:
                property[key] = default_dic[input_dic[key]]
            original_mapping_body = {
                "settings": {
                    "number_of_shards": 5,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": property
                }
            }
            return original_mapping_body

    def update_mapping_build(self, input_dic):
        default_dic = {
                "time": {
                    "type": "date",
                    "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd||date_optional_time"
                },

                "ik_keyword": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    },
                    "analyzer": "ik_smart"
                },
                "keyword": {
                    "type": "text",
                    "fields": {
                        "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                        }
                    }
                },

                "ik": {
                    "type": "text",
                    "analyzer": "ik_smart"

                },

                "location": {
                    "type": "geo_point"
                },

                "id": {
                    "type": "long"
                },

                "json": {
                    "type": "nested"
                },

                "object": {
                    "type": "object"
                }
            }
        property = {}
        for key in input_dic:
            property[key] = default_dic[input_dic[key]]
        original_mapping_body = {
            "properties": property
        }
        return original_mapping_body

    def index_is_exist(self, index_name):
        """
        查询index是否存在
        :param index_name:
        :return:
        """
        res = self.es.indices.exists(index=index_name)
        return res

    def create_index(self, index_name, body=None):
        """
        创建index，如果没有body（mapping类型），创建空mapping索引
        :param index_name:
        :param body:
        :return:
        """
        res = self.es.indices.create(index=index_name, body=body, ignore=400)
        return res

    def put_mapping(self, index_name, body):
        """
        创建修改mapping
        :param index_name:
        :param doc_type:
        :param body:
        :return:
        """
        result = self.es.indices.put_mapping(index=index_name, body=body)
        return result

    def get_mapping(self, index_name):
        """
        创建修改mapping
        :param index_name:
        :param doc_type:
        :param body:
        :return:
        """
        result = self.es.indices.get_mapping(index=index_name)
        return result

    def delete_index(self, index_name):
        """
        删除index
        :param index_name:
        :return:
        """
        res = self.es.indices.delete(index=index_name)
        return res

    def insert_data_list(self, index_name, data_list):
        try:
            """
            批量插入数据
            :param index_name:
            :param data_list:
            :return:
            """
            actions = []
            action = {
                "_index": index_name,
                "_type": "_doc",
                "_source": {
                }
            }
            for i in data_list:
                action["_source"] = i
                actions.append(action)
                action = {
                    "_index": index_name,
                    "_type": "_doc",
                    "_source": {
                    }
                }
            if len(data_list) > 10000:
                for ok, response in helpers.parallel_bulk(self.es, actions, thread_count=4, queue_size=4,
                                                          chunk_size=500, request_timeout=10000):
                    if not ok:
                        print('insert_error')
            else:
                helpers.bulk(self.es, actions, chunk_size=250, request_timeout=10000)
                return 'ok'
        except Exception as e:
            print("etl_service insert_data_list error: ", str(e))

    def up_data_list(self, index_name, id_list, data_list):
        """
        批量修改数据
        :param index_name:
        :param data_list:
        :return:
        """
        # print(index_name, id_list, data_list)
        if len(data_list) == len(id_list):
            actions = []
            action = {
                "_index": index_name,
                "_type": "_doc",
                "_id": "",
                "_source": {
                }
            }

            for i in range(0, len(id_list)):
                action["_id"] = id_list[i]
                old_data = self.search_by_id(index_name, id_list[i])["_source"]
                for key in data_list[i]:
                    old_data[key] = data_list[i][key]
                action["_source"] = old_data
                actions.append(action)
                action = {
                    "_index": index_name,
                    "_type": "_doc",
                    "_source": {
                    }
                }

            # print(actions)
            if len(data_list) > 10000:
                helpers.parallel_bulk(self.es, actions, thread_count=4, queue_size=4, chunk_size=500)

            else:
                helpers.bulk(self.es, actions, chunk_size=250)
            return 'update done'
        else:
            return "length of id_list and data_list didn't match"

    def count_by_body(self, index_name, body):
        count_out = self.es.count(index=index_name, body=body)['count']
        return count_out

    def search_by_body(self, index_name, body, size=""):
        """
        查询数据
        :param index_name:
        :param body:
        :return:
        """
        if size:
            res = self.es.search(index=index_name, body=body, size=size)
        else:
            res = self.es.search(index=index_name, body=body)
        return res

    def search_by_id(self, index_name, id):
        """
        根据id查询数据
        :param index_name:
        :param doc_type:
        :param id:
        :return:
        """

        res = self.es.get(index_name, id=id)
        return res

    def delete_index_data(self, index_name, id):
        """
        删除索引中的一条
        :param index_name:
        :param doc_type:
        :param id:
        :return:
        """

        self.es.delete(index_name, id=id)

    def delete_data_by_body(self, index_name, body):
        """
        删除索引中的一条
        :param index_name:
        :param doc_type:
        :param id:
        :return:
        """

        self.es.delete(index_name, body=body)

    def search_by_body_scan(self, index_name, body):
        """
        查询数据
        :param index_name:
        :param body:
        :return:生成器
        """
        res = helpers.scan(self.es, index_name=index_name, query=body, scroll='5m', timeout="1m")
        return res

    def search_by_geo_distance(self, index_name, body):
        res = helpers.scan(self.es, index_name=index_name, query=body, scroll='5m', timeout="1m")
        return res


def filter_dic_translation(dic_of_search):
    must_list = []
    should_list = []
    must_not_list = []
    sort = []
    for key, value in dic_of_search.items():
        if value["type"] == "text":
            body = {
                "match": {
                    key: {
                        "query": value["value"],
                        "boost": value["boost"]
                    }}}
            should_list.append(body)

        if value["type"] == "multi_term":#  多个字段必须都存在
            if type(value["value"]).__name__ == "list":
                for term_word in value["value"]:
                    body = {
                        "term": {
                            f"{key}.keyword": term_word
                        }}
                    must_list.append(body)
            else:
                body = {
                    "term": {
                        f"{key}.keyword": value["value"]
                    }}
                must_list.append(body)

        if value["type"] == "terms":#  多个字段只要有一个存在即可
            body = {
                "terms": {
                    f"{key}.keyword": value["value"]
                    }}
            must_list.append(body)

        if value["type"] == "term":
            body = {
                "term": {
                    f"{key}.keyword":  value["value"]
                    }}
            must_list.append(body)

        elif value["type"] == "phrase":
            body = {
                "match_phrase": {
                    key: {
                        "query": value["value"]
                    }}}
            must_list.append(body)

        elif value["type"] == "like":
            body = {
                "more_like_this": {"fields": [key],
                                   "like": value["value"],
                                   "min_term_freq": 1,
                                   "max_query_terms": 100}}
            should_list.append(body)

        elif value["type"] == "prefix":
            body = {
                "match_phrase_prefix": {
                    f"{key}.keyword": value["value"]

                }}
            must_list.append(body)

        elif value["type"] == "keyword":
            body = {
                "terms": {
                    f"{key}.keyword": value["value"]

                }}
            must_list.append(body)

        elif value["type"] == "not_keyword":
            body = {
                "terms": {
                    f"{key}.keyword": value["value"]

                }}
            must_not_list.append(body)

        elif value["type"] == "time":
            body = {
                "range": {
                    key: {
                        "lt": value["end_time"],
                        "gt": value["start_time"],
                        "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd"
                    }}}
            must_list.append(body)
        elif value["type"] == "location":
            body = {
                "constant_score": {
                    "filter": {
                        "geo_distance":
                            {"distance": value["distance"],
                             "distance_type": "arc",
                             key:
                                 {"lat": value["lat"],
                                  "lon": value["lon"]
                                  }
                             }
                    }
                }
            }
            must_list.append(body)
        elif value["type"] == "id":
            body = {
                "term": {
                    key: value["value"]}}
            must_list.append(body)

        elif key == 'sort':

            if value["type"] == "distance":
                body = {
                    "_geo_distance": {
                        value["sort"]: {
                            "lat": value["lat"],
                            "lon": value["lon"]
                        },
                        "order": value["asc_desc"],  # asc or desc
                        "unit": "km",
                        "distance_type": "arc"
                    }}
                sort.append(body)
            if value["type"] == "normal":
                body = {

                    f'{value["sort"]}': {
                        "order": value["asc_desc"]
                    }

                }
                sort.append(body)

            if value["type"] == "keyword":
                try:
                    body = {

                        f'{value["sort"]}.keyword': {
                            "order": value["asc_desc"]
                        }

                    }
                    sort.append(body)
                except:
                    print("no_keyword")


    if should_list:
        must_list.append({"bool": {"should": should_list}})

    return must_list, must_not_list, sort


# 自定义搜索querybody分页
def get_custom_query_pagiantion(dic_of_search, current_page, page_size):
    """输入dic_of_search 生成es query
        dic_of_search格式
        {"name"：{"type"：，"value"：，"boost"：}}
        """
    must_list, must_not_list, sort = filter_dic_translation(dic_of_search)
    query_body = {
        "from": (current_page - 1) * page_size,
        "size": page_size,
        "query": {
            "bool": {
                "must": [body for body in must_list
                         ],
                "must_not": [
                    body for body in must_not_list
                ]
            }
        }
    }
    if sort:
        query_body["sort"] = sort
    return query_body


# 自定义搜索querybody
def get_custom_query(dic_of_search):
    """输入dic_of_search 生成es query
    dic_of_search格式
    {"name"：{"type"：，"value"：，"boost"：}}
    """
    must_list, must_not_list, sort = filter_dic_translation(dic_of_search)
    query_body = {
        "query": {
            "bool": {
                "must": [body for body in must_list
                         ],
                "must_not": [
                    body for body in must_not_list
                ]
            }
        }
    }
    if sort:
        query_body["sort"] = sort
    print(query_body, flush=True)
    return query_body


# 分页自定义搜索
def es_custom_search_pagination(index, search_json, current_page=1, page_size=10, max_count=100000):
    if search_json:
        es = ESService(host=ES_SERVER_IP, port=ES_SERVER_PORT)  # http_auth=('elastic', 'changeme'))
        current_page = min(current_page, int(max_count / page_size))
        custom_body = get_custom_query_pagiantion(search_json, current_page, page_size)
        res = es.search_by_body(index_name=index, body=custom_body)
        search_result_list = res['hits']['hits']
        count_body = {}
        count_body['query'] = custom_body['query']
        total_count = es.count_by_body(index_name=index, body=count_body)
        # search_count = res['hits']['total']['value']
        return search_result_list, total_count, math.ceil(total_count / page_size)
    else:
        return [], 0, 0


# 自定义搜索
def es_custom_search(index, search_json):
    if search_json:
        es = ESService(host=ES_SERVER_IP, port=ES_SERVER_PORT)  # , http_auth=('elastic', 'changeme'))
        custom_body = get_custom_query(search_json)
        res = es.search_by_body(index_name=index, body=custom_body, size=10000)
        search_result_list = res['hits']['hits']
        count_body = {}
        count_body['query'] = custom_body['query']
        total_count = es.count_by_body(index_name=index, body=count_body)
        # search_count = res['hits']['total']['value']
        return search_result_list, total_count
    else:
        return [], 0
