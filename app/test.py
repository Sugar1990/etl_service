# # -*- coding:utf-8 -*-
# # author: Scandium
# # work_location: Bei Jing
# # File : test.py
# # time: 2020/8/18 16:38
#
# # !/usr/bin/env python
# # -*- encoding: utf-8 -*-
# '''
# @File         :   datamigration_table.py
# @Modify Time  : 2020/8/3$ 15:14$
# @Author       :  Shao Lixu
# @Version      :  1.0
# @Desciption   :
# '''
#
# import psycopg2
# import re
# from pyArango.connection import *
#
#
# def datamigration_table(pg_server_ip, pg_db_port, pg_user_name, pg_user_password, pg_db_name, pg_table_name,
#                         arango_server_ip, arango_port, arango_user_name, arango_user_password, arango_db_name,
#                         arango_collection, field_tuple, align_field='', fst_com_index=0, sec_com_index=0):
#     """
#     :param sec_com_index:
#     :param fst_com_index:
#     :param arango_db_name: arango数据库名称
#     :param arango_user_password: arango用户名密码
#     :param arango_user_name: arango用户名
#     :param arango_port: arango端口
#     :param arango_server_ip: arango ip
#     :param pg_db_port: pg数据库端口
#     :param pg_server_ip: pg数据库ip
#     :param pg_db_name: pg数据库名称
#     :param pg_user_password: pg用户名密码
#     :param pg_user_name: pg数据库用户名
#     :param pg_table_name: pg数据库表名(单表迁移时，类型是字符串，多表融合时，type(pg_table_name)是个list)
#     :param arango_collection: 导入数据后在arango中的集合名称
#     :param field_tuple: ('pg_field', 'ar_field', 'ifParse')字段对列表
#            [('id','id', '0'), ('name', 'name'), ('other', ['length', 'director', 'showtime'])]
#     :param align_field: 对齐表数据的字段（主键）
#     :return:
#     """
#     # 连接pg数据库
#
#     cur = PG_conn.cursor()
#
#     # 连接arango数据库
#     Aran_conn = Connection(arangoURL='http://%s:%s' % (arango_server_ip, arango_port),
#                            username=arango_user_name, password=arango_user_password)
#     db = Aran_conn[arango_db_name]
#
#
#     # pg_table_name = pg_table_name.split(',')
#     # 从pg中获取数据表
#     # if not isinstance(field_tuple, list):
#     #     raise Exception(u'传入参数要求是列表类型,请检查传入参数类型!')
#
#     pg_table_field = str(field_tuple[0][0]) + ''.join(
#         [',' + str(field_tuple[i][0]) for i in range(1, len(field_tuple))])
#     for tu_field in field_tuple:
#         # print(tu_field[2])
#         if '1' in tu_field:  # pg表中有需要解析的字段，arango字段是列表格式
#             arango_collection_field = str(field_tuple[0][1]) + ''.join(
#                 [',' + str(field_tuple[i][1]) for i in range(1, len(field_tuple))])
#             arango_collection_field = arango_collection_field.replace('[', '')
#             arango_collection_field = arango_collection_field.replace(']', '')
#             arango_collection_field = arango_collection_field.replace('\'', '')
#         else:  # pg中无待解析字段
#             arango_collection_field = str(field_tuple[0][1]) + ''.join(
#                 [',' + str(field_tuple[i][1]) for i in range(1, len(field_tuple))])
#     # print(pg_table_field)
#     # print(arango_collection_field)
#     # 若需要抽取pg实体表中所有字段，用户可不输入pg_table_field，默认为'*'
#     if pg_table_field is None:
#         pg_table_field = '*'
#     if align_field is '':  # 对齐字段为空，单表迁移
#         # 从pg数据库中获取实体表数据
#         sql = "SELECT %s from %s" % (pg_table_field, pg_table_name)
#     else:  # 多表迁移，sql语句中根据align_field添加where语句
#         table1 = align_field[0][0]
#         # print(table1)
#         table2 = align_field[0][1]
#         # print(table2)
#         pg_table_name = str(pg_table_name[0]) + ''.join(
#             [',' + str(pg_table_name[1])])  # for i in range(1, len(pg_table_name))])
#         # sql = "SELECT %s from %s where %s.%s = %s.%s" % (pg_table_field, pg_table_name, table1, align_field[1][0], table2, align_field[1][1])
#         sql = "SELECT %s from %s where %s = %s" % (
#             pg_table_field, pg_table_name, table1 + '.' + align_field[1][0], table2 + '.' + align_field[1][1])
#         print(sql)
#
#     cur.execute(sql)
#     rows = cur.fetchall()
#     # print(rows)
#     PG_conn.close()
#
#     # 在arango中创建集合
#     if not db.hasCollection(arango_collection):
#         db.createCollection(name=arango_collection)
#         print('创建成功')
#     else:
#         print('集合已存在')
#     collection = db[arango_collection]
#
#     # 处理从PG中取出来的数据（包含json）
#     i = 1
#     for row in rows:
#         if i == 10:
#             break
#         # print(row)
#         row = list(row)
#         if fst_com_index != 0 and sec_com_index != 0:
#             for k, v in row[sec_com_index].items():
#                 row[fst_com_index][k] = v
#             del row[sec_com_index]
#         value_list = []  # 存储row里的值
#         print(row)
#         for attr in row:
#             if '1' in field_tuple and isinstance(attr, dict):  # 判断某个字段的值是否是dict类型（即包含多个键值对）
#                 for value in attr.values():
#                     value_list.append(value)
#             if '1' not in field_tuple and isinstance(attr, dict):
#                 value_list.append(attr)
#             else:
#                 value_list.append(attr)
#         # print(value_list)
#         a = dict(zip(arango_collection_field.split(','), value_list))
#         collection.createDocument(a).save()
#         i += 1
#     print('collection count after insert: %s' % collection.count())
#     return 'ok'
#
# # 此行之下 皆为测试
#
# from conf import PG_USER_NAME,PG_USER_PASSWORD,PG_DB_SERVER_IP,PG_DB_PORT,PG_DB_NAME
#
# PG_conn = psycopg2.connect(database=PG_DB_NAME, user=PG_USER_NAME, password=PG_USER_PASSWORD,
#                        host=PG_DB_SERVER_IP, port=PG_DB_PORT)
#
# cur = PG_conn.cursor()
#
# field_tuple=[('id', 'id', '0'), ('title', 'title', '0'), ('summary', 'summary', '0'),
#                                             ('wiki_title', 'wiki_title', '0'), ('synonyms', 'synonms', '0'),
#                                             ('source', 'source', '0'), ('category', 'category', '0')]
#
#
# pg_table_field = str(field_tuple[0][0]) + ''.join(
#         [',' + str(field_tuple[i][0]) for i in range(1, len(field_tuple))])
#
# for tu_field in field_tuple:
#     # print(tu_field[2])
#     if '1' in tu_field:  # pg表中有需要解析的字段，arango字段是列表格式
#         arango_collection_field = str(field_tuple[0][1]) + ''.join(
#             [',' + str(field_tuple[i][1]) for i in range(1, len(field_tuple))])
#         arango_collection_field = arango_collection_field.replace('[', '')
#         arango_collection_field = arango_collection_field.replace(']', '')
#         arango_collection_field = arango_collection_field.replace('\'', '')
#     else:  # pg中无待解析字段
#         arango_collection_field = str(field_tuple[0][1]) + ''.join(
#             [',' + str(field_tuple[i][1]) for i in range(1, len(field_tuple))])
#
#
#     # print(pg_table_field)
#     # print(arango_collection_field)
#     # 若需要抽取pg实体表中所有字段，用户可不输入pg_table_field，默认为'*'
# if pg_table_field is None:
#     pg_table_field = '*'
#
# align_field = ''
# pg_table_name  = 'kg_wiki'
#
# if align_field is '':  # 对齐字段为空，单表迁移
#     # 从pg数据库中获取实体表数据
#     sql = "SELECT %s from %s" % (pg_table_field, pg_table_name)
#
# else:  # 多表迁移，sql语句中根据align_field添加where语句
#     table1 = align_field[0][0]
#     # print(table1)
#     table2 = align_field[0][1]
#     # print(table2)
#     pg_table_name = str(pg_table_name[0]) + ''.join(
#         [',' + str(pg_table_name[1])])  # for i in range(1, len(pg_table_name))])
#     # sql = "SELECT %s from %s where %s.%s = %s.%s" % (pg_table_field, pg_table_name, table1, align_field[1][0], table2, align_field[1][1])
#     sql = "SELECT %s from %s where %s = %s" % (
#         pg_table_field, pg_table_name, table1 + '.' + align_field[1][0], table2 + '.' + align_field[1][1])
#     print(sql)
#
#     cur.execute(sql)
#
#     rows = cur.fetchall()
#     # print(rows)
#     PG_conn.close()
#
# def sql_build():
#     if align_field is '':  # 对齐字段为空，单表迁移
#         # 从pg数据库中获取实体表数据
#         sql = "SELECT %s from %s" % (pg_table_field, pg_table_name)
#
#
# input_pg_dict = list(pgM_dict.keys())
#
# def tranlate_dict(input_pg_dict):
#     align_field = []
#     union_dic = {}
#     union_field = []
#     normal_field = []
#     input_keys = []
#     table_list = []
#     union_num = 0
#     for input_key in input_pg_dict:
#         col_type =input_pg_dict[input_key]["Type_of_col"]
#         if col_type == 'align':
#             for table_name in input_pg_dict[input_key]:
#                 if table_name != "Type_of_col":
#                     align_field.append(f"{table_name}.{input_pg_dict[input_key][table_name]}")
#                     table_list.append(table_name)
#         elif col_type == 'union':
#             for table_name in input_pg_dict[input_key]:
#                 if table_name != "Type_of_col":
#                     union_sql = f"{table_name}.{input_pg_dict[input_key][table_name]}"
#                     union_field.append(union_sql)
#                     if input_key in union_dic:
#                         union_dic[input_key][-1]+=1
#                         union_num += 1
#                     else:
#                         union_dic[input_key]=[union_num,union_num]
#             # union_set_list.append(input_key)
#         elif col_type == '':
#             for table_name in input_pg_dict[input_key]:
#                 if table_name != "Type_of_col":
#                     normal_sql = f"{table_name}.{input_pg_dict[input_key][table_name]}"
#                     normal_field.append(normal_sql)
#             input_keys.append(input_key)
#     start_un = len(normal_field)+1
#     for union_key in union_dic:
#         union_dic[union_key][0] += start_un
#         union_dic[union_key][1] += start_un
#     if align_field:
#         sql_align = ''
#         for align_sql_num in range(len(align_field)-1):
#             sql_align += f'AND {align_field[align_sql_num] } = {align_field[align_sql_num+1]}'
#         where_align = 'WHERE' + sql_align[3:]
#
#     normal_sql_fin = ','.join(normal_field) + ','if normal_field else ''
#
#     union_sql_fin = ','.join(union_field) if union_field else ''
#
#     # if normal_field:
#     #     normal_sql_fin = ','.join(normal_field)
#     # else:
#     #     normal_sql_fin = ''
#     print(normal_sql_fin)
#     print(align_field[0])
#     sql_total = f"SELECT {align_field[0]+','}{normal_sql_fin}{union_sql_fin} From {','.join(table_list)} {where_align}"
#     return sql_total,union_dic
#
# # return align_field, normal_field, union_field
#
# pgM_dict = {
#     'id':{"Type_of_col":'align',
#           "table_name1":"id","table_name2":"id"},
#     'syn':{"Type_of_col":'union',
#           "table_name1":"syn","table_name2":"syn","table_name2":"syn3"}
#
# }
#
# tranlate_dict(pgM_dict)
#
# {'id': {"Type_of_col": 'align',
#             "entity": "id"},
# 'name': {"Type_of_col": '',
#               "entity": "name"
# },
# 'synonyms': {"Type_of_col": '',
#         "entity": 'synonyms'},
# }
#
# mapping_json
#
# {"id":"id","name":"ik_keyword" ,'synonyms':"ik_keyword"}
#
#
# #
# # PG_USER_NAME = 'postgres'
# # PG_USER_PASSWORD = '123456'
# # PG_DB_SERVER_IP = '192.168.2.66'
# # PG_DB_PORT = '5432'
# # PG_DB_NAME = 'Tagging System'
# # @blue_print.route('JsonInsert')
# # def json_insert():
# #
# #
# # @blue_print.route('/ArdataMigration', methods=['GET'])
# # def ar_migration():
# #
# #
# # @blue_print.route('/dataMigration/table', methods=['GET'])
# # def new_migration():
# #     pg_server_ip = request.args.get('pgIP', '127.0.0.1')
# #     pg_db_port = request.args.get('pg_port', '5432')
# #     pg_user_name = request.args.get('pg_user', 'postgres')
# #     pg_user_password = request.args.get('pg_psw', 'slx123456')
# #     pg_db_name = request.args.get('pg_db_name', 'newdb')
# #     pg_table_name = request.args.get('pg_table_name', '')  # 多表迁移时是list类型
# #     arango_server_ip = request.args.get('arIP', '127.0.0.1')
# #     arango_port = request.args.get('ar_port', '8529')
# #     arango_user_name = request.args.get('ar_user', 'root')
# #     arango_user_password = request.args.get('ar_psw', '')
# #     arango_db_name = request.args.get('ar_db_name', 'mydb')
# #     arango_collection = request.args.get('collection', 'movie_show_city')
# #     # f_tuple = [('id', 'id', '0'), ('name', 'name', '0'), ('other', ['length', 'director', 'showtime'], '1')]
# #     f_tuple_a=[('movie.id', 'movie_id', '0'), ('movie.name', 'movie_name', '0'), ('cityid', 'city_id', '0'), ('city.name', 'city_name', '0')]
# #     field_tuple = request.args.get('field_tuple', f_tuple_a)
# #     align_ex = [('movie', 'city'), ('cityid', 'id')]
# #     align_field = request.args.get('align_field', align_ex)
# #     fst_com_index = request.args.get('fst_com_index', 0)
# #     sec_com_index = request.args.get('sec_com_index', 0)
# #     print(request.args)
# #     # postman参数解析
# #     pg_table_name = parse_table_name(pg_table_name)
# #     field_tuple = parse_field_tuple(field_tuple)
# #     align_field = parse_align_field(align_field)
# #
# #     result = datamigration_table(pg_server_ip, pg_db_port, pg_user_name, pg_user_password, pg_db_name, pg_table_name,
# #                                  arango_server_ip, arango_port, arango_user_name, arango_user_password, arango_db_name,
# #                                  arango_collection, field_tuple, align_field, fst_com_index, sec_com_index)
# #     return after_response(0, "datamigration_table", input_data = result)
# #
# #
# # @blue_print.route('/CreateEdge', methods=['GET'])
# # def home():
# #     # 连接PG参数
# #     PG_USER_NAME = request.args.get('PG_USER_NAME', default='postgres')
# #     PG_USER_PASSWORD = request.args.get('PG_USER_PASSWORD', 'slx123456')
# #     PG_DB_SERVER_IP = request.args.get('PG_DB_SERVER_IP', '127.0.0.1')
# #     PG_DB_PORT = request.args.get('PG_DB_PORT', '5432')
# #     PG_DB_NAME = request.args.get('PG_DB_NAME', 'newdb')
# #     # 连接ArangoDB参数
# #     ARANGO_SERVER_IP = request.args.get('ARANGO_SERVER_IP', 'http://127.0.0.1')
# #     ARANGO_PORT = request.args.get('ARANGO_PORT', '8529')
# #     ARANGO_USER_NAME = request.args.get('ARANGO_USER_NAME', 'root')
# #     ARANGO_USER_PASSWORD = request.args.get('ARANGO_USER_PASSWORD', '')
# #     ARANGO_DB_NAME = request.args.get('ARANGO_DB_NAME', 'mydb')
# #     # 边参数
# #     PG_TABLE = request.args.get('PG_TABLE', 'movie_actor')
# #     FROM_FIELD = request.args.get('FROM_FIELD', 'movieid')
# #     TO_FIELD = request.args.get('TO_FIELD', 'actorid')
# #     ARANGO_EDGES_COLLECTION = request.args.get('ARANGO_EDGES_COLLECTION', 'movie_actor2')
# #     ARANGO_FROM_COLLECTION = request.args.get('ARANGO_FROM_COLLECTION', 'movie')
# #     ARANGO_TO_COLLECTION = request.args.get('ARANGO_TO_COLLECTION', 'actor')
# #     AR_FROM_FIELD = request.args.get('AR_FROM_FIELD', 'id')
# #     AR_TO_FIELD = request.args.get('AR_TO_FIELD', 'id')
# #     NEED_PARSE_FIELD = request.args.get('NEED_PARSE_FIELD', "[]")
# #     if NEED_PARSE_FIELD == '[]':
# #         NEED_PARSE_FIELD = []
# #     else:
# #         NEED_PARSE_FIELD.replace("[", "").replace("]", "").split(",")
# #
# #     print(PG_USER_NAME, PG_USER_PASSWORD, PG_DB_SERVER_IP, PG_DB_PORT, PG_DB_NAME,
# #           ARANGO_SERVER_IP, ARANGO_PORT, ARANGO_USER_NAME, ARANGO_USER_PASSWORD, ARANGO_DB_NAME,
# #           PG_TABLE, FROM_FIELD, TO_FIELD, ARANGO_EDGES_COLLECTION, ARANGO_FROM_COLLECTION, ARANGO_TO_COLLECTION,
# #           AR_FROM_FIELD, AR_TO_FIELD, NEED_PARSE_FIELD)
# #     # 执行业务操作
# #     # mains(PG_USER_NAME, PG_USER_PASSWORD, PG_DB_SERVER_IP, PG_DB_PORT, PG_DB_NAME,
# #     #       ARANGO_SERVER_IP, ARANGO_PORT, ARANGO_USER_NAME, ARANGO_USER_PASSWORD, ARANGO_DB_NAME,
# #     #       PG_TABLE, FROM_FIELD, TO_FIELD, ARANGO_EDGES_COLLECTION, ARANGO_FROM_COLLECTION, ARANGO_TO_COLLECTION,
# #     #       AR_FROM_FIELD, AR_TO_FIELD, NEED_PARSE_FIELD)
# #     mains(PG_USER_NAME, PG_USER_PASSWORD, PG_DB_SERVER_IP, PG_DB_PORT, PG_DB_NAME,
# #           PG_TABLE, FROM_FIELD, TO_FIELD, NEED_PARSE_FIELD,
# #           ARANGO_EDGES_COLLECTION, AR_FROM_FIELD, ARANGO_FROM_COLLECTION, AR_TO_FIELD, ARANGO_TO_COLLECTION)
# #     return '创建了边集合'