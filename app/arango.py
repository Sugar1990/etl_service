# coding=UTF-8

from flask import Blueprint
from flask import request, jsonify
import json
from arango_service import create_collection, delete_collection, insert_data, query_data, delete_data, \
    edge_collection_insert, edge_delete, edge_query_data,get_data_id

api_AR = Blueprint('arango', __name__)
blue_print = api_AR

@blue_print.route('/createCollection', methods=['GET'])
def new_collection():
    name = request.args.get('collection','test_collection')
    type = request.args.get('type', '')
    result = create_collection(name,type)
    return after_response(0, result)


@blue_print.route('/deleteCollection', methods=['GET'])
def drop_collection():
    name = request.args.get('collection','test_collection')
    result = delete_collection(name)
    return after_response(0, result)


@blue_print.route('/insertData', methods=['GET'])
def creat_data():
    collection_name = request.args.get('collection','test_collection')
    data = request.args.get('data',[
        {"name": "Robert", "surname": "Baratheon", "alive": False, "traits": ["A", "H", "C"]},
        {"name": "Jaime", "surname": "Lannister", "alive": True, "age": 36, "traits": ["A", "F", "B"]},])
    result = insert_data(collection_name, data)
    return after_response(0, result)


@blue_print.route('/queryData', methods=['GET'])
def data_list():
    collection_name = request.args.get('collection', 'test_collection')
    filter_dict = request.args.get('filter_dict','')
    filter_dict = eval(filter_dict)
    result = query_data(collection_name, filter_dict)
    if result:
        return after_response(0,"search_done", input_data = result)
    else:
        return after_response(1,"no_search_result", input_data = result)


@blue_print.route('/deleteData', methods=['GET'])
def collection_data_delete():
    name = request.args.get('collection','test_collection')
    key = request.args.get('key', 'all')
    result = delete_data(name, key)
    return after_response(0 , "data_delete", input_data = result)


@blue_print.route('/insertEdge', methods=['GET'])
def creat_edge():
    collection_name = request.args.get('collection','test_collection')
    edge = request.args.get('edge','''[
        {"from":'test_data/name2', "to":'test_data/name3','_source':{
            "name": "Robert", "surname": "Baratheon", "alive": False
        }}
    ]''')
    # edge = json.load(edge)
    result = edge_collection_insert(collection_name, edge)
    return after_response(0, result)


@blue_print.route('/deleteEdge', methods=['GET'])
def collection_edge_delete():
    name = request.args.get('collection','test_collection')
    key = request.args.get('key', 'all')
    result = edge_delete(name, key)
    return after_response(0 , "data_delete", input_data = result)


@blue_print.route('/queryEdge', methods=['GET'])
def edge_list():
    collection_name = request.args.get('collection', 'test')
    filter_dict = request.args.get('filter_dict','')
    filter_dict = eval(filter_dict)
    result = edge_query_data(collection_name, filter_dict = filter_dict)
    if result:
        return after_response(0, "search_done", input_data=result)
    else:
        return after_response(1, "no_search_result", input_data=result)


@blue_print.route('/queryId', methods=['GET'])
def get_id_list():
    collection_name = request.args.get('collection', 'test')
    filter_dict = request.args.get('filter_dict', '{}')
    filter_dict = eval(filter_dict)
    result = get_data_id(collection_name, filter_dict=filter_dict)
    if result:
        return after_response(0, "search_done", input_data=result)
    else:
        return after_response(1, "no_search_result", input_data=result)


def after_response(input_code, input_message, input_data='None'):
    json_output = {
        "code":  input_code,
        "massage": input_message,
        "data": input_data
    }
    return jsonify(json_output)