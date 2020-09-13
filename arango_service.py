from pyArango.connection import *

from conf import AR_SERVER_IP, AR_SERVER_PORT, AR_USER_NAME, AR_USER_PASSWORD, AR_DB_NAME


class ArConn(object):
    __instance = None

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not cls.__instance:
            cls.__instance = object.__new__(cls, *args, **kwargs)
            ar_conn = Connection(arangoURL='http://%s:%s' % (AR_SERVER_IP, AR_SERVER_PORT), username=AR_USER_NAME,
                                 password=AR_USER_PASSWORD)
            cls.__instance.ar = ar_conn[AR_DB_NAME]

        return cls.__instance


def create_collection(name, edge=False):
    """
    创建集合
    :param name:
    :return:
    """
    if name not in ArConn().ar.collections.keys():
        if edge:
            ArConn().ar.createCollection(name=name, className="Edges")
        else:
            ArConn().ar.createCollection(name=name, className="Collection")
    return 'ok'


def delete_collection(name):
    """
    创建集合
    :param name:
    :return:
    """
    if name in ArConn().ar.collections.keys():
        ArConn().ar[name].delete()
    return 'ok'


def insert_data(collection_name, data):
    """
    新增数据
    :param collection_name:
    :param data:
    :return:
    """
    if collection_name in ArConn().ar.collections.keys():
        collection = ArConn().ar[collection_name]
        for row in data:
            collection.createDocument(row).save()
        return "ok"
    else:
        return "collection不存在"


def query_data(collection_name, filter_dict=None, page_index=None, page_size=20):
    """
    查询数据
    :param collection_name:
    :param filter_dict:
    :param page_index:
    :param page_size:
    :return:
    """

    def parse_paging(page_index=None, page_size=20):
        """
        分页处理
        :param page_index:
        :param page_size:
        :return:
        """
        skip = (page_index - 1) * page_size if page_index else 0
        limit = page_size
        return skip, limit

    collection = ArConn().ar[collection_name]
    skip, limit = parse_paging(page_index, page_size)
    if filter_dict:
        query = collection.fetchByExample(filter_dict, batchSize=10, count=True, skip=skip, limit=limit)
    else:
        query = collection.fetchAll(skip=skip, limit=limit)
    return query.response['result']


def delete_data(collection_name, key=None):
    """
    查询数据
    :param collection_name:
    :param key:
    :return:
    """
    if key:
        if collection_name in ArConn().ar.collections.keys():
            collection = ArConn().ar[collection_name]
            if key == 'all':
                query = collection.fetchAll()
                for doc in query:
                    doc.delete()
                return "all data has been deleted"
            else:
                data = collection[key]
                data.delete()
                return "ok"
        else:
            return "collection不存在"
    else:
        return "No id key insert"


def edge_collection_insert(collection_name, data):
    data = eval(data)
    if collection_name in ArConn().ar.collections.keys():
        collection_type = ArConn().ar[collection_name].getType()
        if collection_type == 'edge':
            collection = ArConn().ar[collection_name]
            for row in data:
                edge_insert = collection.createEdge_(row['_data'])
                edge_insert._from = row['_from']
                edge_insert._to = row['_to']
                edge_insert.save()
            return "ok"
        else:
            return "collection属性不为edge"
    else:
        return "collection不存在"


def edge_delete(collection_name, key=None):
    if key:
        if collection_name in ArConn().ar.collections.keys():
            collection = ArConn().ar[collection_name]
            if key == 'all':
                query = collection.fetchAll()
                for doc in query:
                    doc.delete()
                return "all data has been deleted"
            else:
                data = collection[key]
                data.delete()
                return "ok"
        else:
            return "collection不存在"
    else:
        return "No id key insert"


def edge_query_data(collection_name, filter_dict=None, page_index=None, page_size=20):
    """
    查询数据
    :param collection_name:
    :param filter_dict:
    :param page_index:
    :param page_size:
    :return:
    """

    def parse_paging(page_index=None, page_size=20):
        """
        分页处理
        :param page_index:
        :param page_size:
        :return:
        """
        skip = (page_index - 1) * page_size if page_index else 0
        limit = page_size
        return skip, limit

    collection = ArConn().ar[collection_name]
    skip, limit = parse_paging(page_index, page_size)
    if filter_dict:
        print(filter_dict)
        query = collection.fetchByExample(filter_dict, batchSize=10, count=True, skip=skip, limit=limit)
    else:
        query = collection.fetchAll(skip=skip, limit=limit)
    return query.response['result']


def get_data_id(collection_name, filter_dict=None):
    collection = ArConn().ar[collection_name]
    if filter_dict:
        query = collection.fetchByExample(filter_dict, batchSize=10, count=True)
    else:
        query = collection.fetchAll()
    return [res['_id'] for res in query.response['result']]


def query_father(collection, field, value, edge_collection):
    """
    查询一个节点的父亲节点
    :param collection: 实体collection
    :param field: 要查询的属性
    :param value: 属性的值
    :param edge_collection: 边collection
    :return:
    """
    aql = """
            FOR c IN %s
            FILTER c.%s == "%s"
            FOR v IN 1..1 OUTBOUND c %s
                RETURN v
            """ % (collection, field, value, edge_collection)
    queryResult = ArConn().ar.AQLQuery(aql)
    result = queryResult.response['result']
    return result


def query_child(collection, field, value, edge_collection):
    """
    查询一个节点的孩子节点
    :param collection: 实体collection
    :param field: 要查询的属性
    :param value: 属性的值
    :param edge_collection: 边collection
    :return:
    """
    aql = """
            FOR c IN %s
            FILTER c.city == "%s"
            FOR v IN 1..1 OUTBOUND c %s
                RETURN %s
            """ % (collection, field, value, edge_collection)
    queryResult = ArConn().ar.AQLQuery(aql)
    result = queryResult.response['result']
    return result

# query_child('test_data','alive',"false","lk_test3")
#
#
# get_data_id('test_data',{'name':'lk'})
#
# def get_edges(collection_name,dot_id):
#     collection = ArConn().ar[collection_name]
#     return collection.getEdges(f"{collection_name}/{dot_id}")

# collection = ArConn().ar["test_data"]
# if 'lk_test3' in ArConn().ar.collections.keys():
#     print('done')
