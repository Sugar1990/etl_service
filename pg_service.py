# -*- coding:utf-8 -*-
# author: Scandium
# work_location: Bei Jing
# File : pg_migration_service.py
# time: 2020/8/19 1:23

from itertools import chain
import json
import psycopg2

from conf import ES_SERVER_IP, ES_SERVER_PORT, AR_SERVER_IP, AR_SERVER_PORT, PG_DB_NAME, PG_USER_NAME, PG_USER_PASSWORD, \
    PG_DB_SERVER_IP, PG_DB_PORT


def tranlate_dict(input_pg_dict):
    align_field = []
    union_dic = {}
    union_field = []
    normal_field = []
    input_keys = []
    table_list = []
    union_num = 0
    for input_key in input_pg_dict:
        col_type = input_pg_dict[input_key]["col_type"]
        if col_type == 'align':
            for table_name in input_pg_dict[input_key]:
                if table_name != "col_type":
                    align_field.append(f"{table_name}.{input_pg_dict[input_key][table_name]}")
                    table_list.append(table_name)
            input_keys.insert(0, input_key)
        elif col_type == 'union':
            for table_name in input_pg_dict[input_key]:
                if table_name != "col_type":
                    union_sql = f"{table_name}.{input_pg_dict[input_key][table_name]}"
                    union_field.append(union_sql)
                    if input_key in union_dic:
                        union_dic[input_key][-1] += 1
                        union_num += 1
                    else:
                        union_dic[input_key] = [union_num, union_num]
                        union_num += 1
        elif col_type == '':
            for table_name in input_pg_dict[input_key]:
                if table_name != "col_type":
                    normal_sql = f"{table_name}.{input_pg_dict[input_key][table_name]}"
                    normal_field.append(normal_sql)
            input_keys.append(input_key)
    start_un = len(normal_field) + 1
    for union_key in union_dic:
        union_dic[union_key][0] += start_un
        union_dic[union_key][1] += start_un
    if align_field:
        sql_align = ''
        for align_sql_num in range(len(align_field) - 1):
            sql_align += f' AND {align_field[align_sql_num]} = {align_field[align_sql_num + 1]}'
        if len(align_field) > 1:
            where_align = 'WHERE' + sql_align[4:]
        else:
            where_align = sql_align[4:]
    normal_sql_fin = ','.join(normal_field) + ',' if normal_field else ''
    union_sql_fin = ','.join(union_field) if union_field else ''

    if union_field or normal_field:
        align_sql_fin = align_field[0] + ','
    else:
        align_sql_fin = align_field[0]

    table_total = f'{align_sql_fin}{normal_sql_fin}{union_sql_fin}'.strip(',')
    sql_total = f"SELECT {table_total} From {','.join(table_list)} {where_align}"
    input_keys.extend(
        list(union_dic.keys()))  # [word.split('.')[1] for word in union_field] [align_field[0].split('.')[1]],
    return sql_total, union_dic, input_keys


def list_fusion(input_list):
    fused_list = []
    for fuse_element in input_list:
        list_extend = fuse_element.strip('[],').split(',')
        fused_list.extend(list_extend)
    return list(set(fused_list))


def json_format_judge(input_json):
    list_input = eval(input_json)
    if type(list_input).__name__ == 'list' and type(list_input[0]).__name__ == 'dict':
        return True
    else:
        return False


def excute_sql_fusion(SQL, fusion_dict, key_list):
    PG_conn = psycopg2.connect(database=PG_DB_NAME, user=PG_USER_NAME, password=PG_USER_PASSWORD, host=PG_DB_SERVER_IP,
                               port=PG_DB_PORT)
    try:
        cur = PG_conn.cursor()
        cur.execute(SQL)
        rows = cur.fetchall()
        if rows:
            if fusion_dict:
                data_fused = [list(chain(list(row[:list(fusion_dict.values())[0][0]]),
                                         [list_fusion(row[fusion_dict[fuse_key][0]:fusion_dict[fuse_key][1]]) for
                                          fuse_key in fusion_dict])) for row in rows]
                data_list = [dict(zip(key_list, list(row))) for row in data_fused]
                cur.close()
                return data_list
            else:
                data_list = [dict(zip(key_list, list(row))) for row in rows]
                cur.close()
                return data_list
    except psycopg2.ProgrammingError as exc:
        cur.close()
        return exc

#
# def ar_api_migration(rows_data, col_list, collection_name):
#     url = f'http://{AR_SERVER_IP}:{AR_SERVER_PORT}'
#     para = {"collection": collection_name}
#     creat_status = requests.get(url + '/createCollection', params=para, headers={})
#     data_list = [dict(zip(row_data, col_list)) for row_data in rows_data]
#     para_insert = {"collection": collection_name, "data": json.dumps(data_list)}
#     insert_status = requests.get(url + '/insertData', params=para_insert, headers={})
#     return creat_status.content, insert_status.content
#
#
# #
# # test_dic = {"title": "ik", "entity_id": "id", "content": "ik_keyword", "props": "ik_keyword", "tags": "ik_keyword",
# #          "synonyms": "ik_keyword", "time": "time", "location": "location", "type": "ik_keyword"}
#
#
# def es_api_migration(rows_data, col_list, es_index, es_index_dic):
#     url = f'http://{ES_SERVER_IP}:{ES_SERVER_PORT}'
#     para = {"creat": es_index, "mapping_json": json.dumps(es_index_dic)}
#     creat_status = requests.get(url + '/createIndex', params=para, headers={})
#     data_list = [dict(zip(row_data, col_list)) for row_data in rows_data]
#     para_insert = {"data_insert_index": es_index, "data_insert_json": json.dumps(data_list)}
#     insert_status = requests.get(url + '/insertData', params=para_insert, headers={})
#     return creat_status.content, insert_status.content


#
# pgM_dict = {
#     'id':{"col_type":'align',
#           "kg_wiki":"id","kg_fetched_news":"id","news_corpus":"id"},
#     'title':{"col_type":'union',
#           "kg_wiki":"title","news_corpus":"news_title","kg_fetched_news":"news_title"},
#     'url':{"col_type":'',
#           "kg_wiki":"tags"},
#     'title1':{"col_type":'union',
#           "kg_wiki":"title","news_corpus":"news_title","kg_fetched_news":"news_title"},
# }
