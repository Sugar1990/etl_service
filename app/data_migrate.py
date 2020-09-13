# -*- coding:utf-8 -*-
# author: Scandium
# work_location: Bei Jing
# File : data_migrate.py
# time: 2020/8/18 14:26

import json

from flask import Blueprint
from flask import request, jsonify

from conf import ES_SERVER_IP, ES_SERVER_PORT
from es_service import ESService  # es_custom_search_pagination, es_custom_search
from pg_service import excute_sql_fusion, tranlate_dict, json_format_judge
from .utils import success_res, fail_res

api_PG = Blueprint('pg_migration', __name__)
blue_print = api_PG


# pg数据抽取
@blue_print.route('/pg_extraction', methods=['POST'])
def pg_extraction():
    try:
        pg_dict = request.json.get('pg_dict', {})
        sql, fusion_dict, key_list = tranlate_dict(pg_dict)
        data_result = excute_sql_fusion(sql, fusion_dict, key_list)
        return success_res('extract_done', data=data_result)
    except:
        return fail_res('extract_error')


# JSON数据格式判断
@blue_print.route('/json_judge', methods=['POST'])
def json_judge():
    try:
        json_input = request.json.get('json_input', {})
        json_bool = json_format_judge(json_input)
        return success_res('judge_done', data=json_bool)
    except:
        return fail_res('judge_error')


# pg数据转接ES数据库
@blue_print.route('/pg_insert_es', methods=['POST'])
def pg_insert_es():
    try:
        pg_dict = request.json.get('pg_dict', {})
        es_index_name = request.json.get('es_index_name', '')
        es_mapping_dic = request.json.get('es_mapping_dict', {})
        sql, fusion_dict, key_list = tranlate_dict(pg_dict)
        try:
            data_result = excute_sql_fusion(sql, fusion_dict, key_list)
        except Exception as e:
            print("excute_sql_fusion error: ", str(e), flush=True)
            data_result = []
        if es_index_name:
            es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
            index_name = es_index_name
            if not es_service.index_is_exist(index_name):
                mapping_json = es_mapping_dic
                if mapping_json:
                    creat_result = es_service.create_index(index_name, es_service.creat_mapping_build(es_mapping_dic))
                else:
                    creat_result = es_service.create_index(index_name)

                if creat_result:
                    print("Creat done", flush=True)
            else:
                print("index name already exist", flush=True)
            es_service = ESService(ES_SERVER_IP, port=ES_SERVER_PORT)
            if data_result:
                data_insert_result = es_service.insert_data_list(index_name, data_result)
                print("data_insert_result: ", data_insert_result)
            return success_res("Insert done", data=data_insert_result)
        else:
            return fail_res("Need es_index")
    except Exception as e:
        print(str(e), flush=True)
        return fail_res("migration fail")

# # pg数据转接ar数据库实体
# @blue_print.route('/pg_insert_ar_doc', methods=['POST'])
#
#
# # pg数据转接ar数据库实体
# @blue_print.route('/pg_insert_ar_edge', methods=['POST'])
