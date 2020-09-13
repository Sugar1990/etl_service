import json

from flask import Blueprint
from flask import request, jsonify

from conf import ES_SERVER_IP, ES_SERVER_PORT
from es_service import ESService, es_custom_search_pagination, es_custom_search
from .utils import success_res, fail_res

api_ES = Blueprint('elasticsearch', __name__)
blue_print = api_ES


# 创建索引，带mapping
@blue_print.route('/createIndex', methods=['POST'])
def creat_index():
    """
        判断index是否存在如果存在，返回‘already_exsist’
        否则按照给定json创建索引
    """
    try:
        index_name = request.json.get('create_index', '')
        if index_name:
            es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
            if not es_service.index_is_exist(index_name):
                mapping_json = request.json.get('mapping_json', '')
                if mapping_json != '':
                    creat_result = es_service.create_index(index_name, es_service.creat_mapping_build(mapping_json))
                    print(creat_result, flush=True)
                    creat_result = es_service.create_index(index_name)
                print(creat_result, flush=True)

                if creat_result:
                    res = success_res("Creat done")
            else:
                res = fail_res("Index name already exist")
        else:
            res = fail_res("Need create_index")
    except Exception as e:
        res = fail_res(str(e))
    return jsonify(res)


# 修改更新mapping
@blue_print.route('/updateIndex', methods=['POST'])
def update_index():
    """
        给索引添加或者修改已有字段，按照json传入，在原有mapping基础上增加或修改
    """
    try:
        update_index = request.args.get('update_index', '')
        if update_index:
            es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
            index_name = update_index
            if es_service.index_is_exist(index_name):
                mapping_json = request.args.get('mapping_json', '')
                if mapping_json:
                    mapping_dic = json.loads(mapping_json)
                    update_result = es_service.put_mapping(index_name, es_service.update_mapping_build(mapping_dic))
                else:
                    update_result = es_service.put_mapping(index_name)
                res = success_res("Update done")
            else:
                res = fail_res("index name doesn't exist")
        else:
            res = fail_res("Need update_index")
    except Exception as e:
        res = fail_res(str(e))
    return jsonify(res)


# 删除索引
@blue_print.route('/deleteIndex', methods=['GET'])
def delete_index():
    try:
        delete_index = request.args.get('delete_index', '')
        if delete_index:
            es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
            index_name = delete_index
            if es_service.index_is_exist(index_name):
                delete_result = es_service.delete_index(index_name)
                print(delete_result)
                res = success_res("Delete done")
            else:
                res = fail_res("index name doesn't exist")
        else:
            res = fail_res("Need delete_index")
    except Exception as e:
        print(str(e), flush=True)
        res = fail_res()
    return jsonify(res)


# 数据导入
@blue_print.route('/dataInsert', methods=['POST'])
def data_insert():
    try:
        data_insert_index = request.json.get('data_insert_index', '')
        if data_insert_index:
            es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
            index_name = data_insert_index
            data_insert_list = request.json.get('data_insert_json', '')
            if es_service.index_is_exist(index_name):
                data_insert_result = es_service.insert_data_list(index_name, data_insert_list)
                print(data_insert_result, flush=True)
                res = success_res("Insert done", data=str(data_insert_result))
            else:
                res = fail_res("index name doesn't exist")
        else:
            res = fail_res("Need insert_index")
    except Exception as e:
        print(str(e), flush=True)
        res = fail_res()
    return jsonify(res)


# 自定义分页搜索
@blue_print.route('/searchCustomPagination', methods=['POST'])
def custom_search_pagination():
    """
    从ES中按照自定义字段进行搜索\
    需要输入index
    search_json格式：
    {
    'text_name'：{'type'：‘text’，'value'：''，'boost'：''},
    'time_name'：{'type'：‘time’，'start_time'：''，'end_time'：''}
    'location_name':{'type'：'location','distacne':'','loc':'',’lon‘：’‘}
    ’id_name‘:{'type'：'id','value':''}
    }
    :return:
    """
    try:
        search_index = request.json.get('search_index', '')
        search_json = request.json.get('search_json', {})
        page_size = request.json.get('pageSize', 10)
        current_page = request.json.get('currentPage', 1)
        search_list, total_count, page_count = es_custom_search_pagination(search_index, search_json, current_page,
                                                                           page_size)
        res_list = [item for item in search_list if item]
        # print('search_list: ', search_list, flush=True)
        result = {
            "dataList": res_list,
            "totalCount": total_count,
            "pageCount": page_count,
            "currentPage": current_page,
        }
        res = success_res("search Pagination done", data=result)
        # print(res, flush=True)
    except Exception as e:
        print(str(e), flush=True)
        res = fail_res()
    return jsonify(res)


# 自定义搜索
@blue_print.route('/searchCustom', methods=['POST'])
def custom_search():
    """
    从ES中按照自定义字段进行搜索\
    需要输入index
    search_json格式：
    {
    'text_name'：{'type'：‘text’，'value'：''，'boost'：''},
    'time_name'：{'type'：‘time’，'start_time'：''，'end_time'：''}
    'location_name':{'type'：'location','distacne':'','loc':'',’lon‘：’‘}
    ’id_name‘:{'type'：'id','value':''}
    }
    :return:
    """
    try:
        search_index = request.json.get('search_index', '')
        search_json = request.json.get('search_json', '')
        search_list, total_count = es_custom_search(search_index, search_json)

        res_list = [item for item in search_list if item]

        result = {
            "dataList": res_list,
            "totalCount": total_count,
        }
        print(result, flush=True)
        res = success_res("search done", data=result)
    except Exception as e:
        res = fail_res(str(e))
    return jsonify(res)


@blue_print.route('/searchId', methods=['POST'])
def id_search():
    try:
        search_index = request.json.get('search_index', '')
        search_json = request.json.get('search_json', {})
        search_list, total_count = es_custom_search(search_index, search_json)

        res_list = [item["_id"] for item in search_list if item]

        result = {
            "dataList": res_list,
            "totalCount": total_count,
        }
        res = success_res("Search id done", data=result)
    except Exception as e:
        res = fail_res(str(e))
    return jsonify(res)


@blue_print.route('/deletebyId', methods=['POST'])
def delete_by_id():
    '''
    id_json = ['CKh3f3MB3XABs4xBdjkI', '0_QUe3MBaeUXKBEXeJX1', '0vQUe3MBaeUXKBEXL5WF']
    列表套字符串用英文逗号隔开
    '''
    try:
        delete_index = request.json.get('delete_index', '')
        id_json = request.json.get('id_json', '')
        es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
        idlist = id_json
        if es_service.index_is_exist(delete_index):
            for id in idlist:
                es_service.delete_index_data(delete_index, id)
        res = success_res("Delete done")
    except Exception as e:
        res = fail_res(str(e))
    return jsonify(res)


@blue_print.route('/updatebyId', methods=['POST'])
def update_by_id():
    '''
    id_json = CKh3f3MB3XABs4xBdjkI, 0_QUe3MBaeUXKBEXeJX1, 0vQUe3MBaeUXKBEXL5WF
    用英文逗号隔开
    data_json =
    '''
    try:
        update_index = request.json.get('update_index', '')
        data_update_json = request.json.get("data_update_json", '')
        es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
        data_insert_list = data_update_json
        idlist, data_list = [], []
        for item in data_insert_list:
            idlist.extend(item.keys())
            data_list.extend(item.values())
        if es_service.index_is_exist(update_index):
            es_service.up_data_list(update_index, idlist, data_list)
            res = success_res("update data by id done")
        else:
            res = fail_res("index name doesn't exist")
    except Exception as e:
        res = fail_res(str(e))
    return jsonify(res)
