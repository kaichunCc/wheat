#!/usr/bin/python3

from os import pipe
import pymysql
from pymysql import NULL, cursors

hsag_file = "../rawdata/沪深Ａ股20210731.txt"
shanghai_main_file = "../rawdata/上证主板20210801.txt"
shenzheng_main_file = "../rawdata/深证主板20210801.txt"
kcb_file = "../rawdata/科创板20210801.txt"
cyb_file = "../rawdata/创业板20210801.txt"

db = NULL
cursor = NULL

def connect_db():
	global db
	global cursor
	
	db = pymysql.connect(host="localhost", port=3306, user="root", passwd="admin", db="tdx")
	cursor = db.cursor()

def disconnect_db():
	db.close()

def test_pymysql():
	db = pymysql.connect(host="localhost", port=3306, user="root", passwd="admin", db="tdx")
	cursor = db.cursor()
	cursor.execute("SELECT VERSION()")

	data=cursor.fetchone()
	print("Database version: %s" % data)#
	db.close()
	print("hello")

def read_file(file_name):
	fd = open(file=file_name, mode='r')
	
	cnt = 0
	while cnt < 2:
		line = fd.readline()
		cnt += 1
		
	columns = line.split("\t")

	index = 1
	for column in columns:
		print("id %d : %s, %d" % (index, column, len(column)))
		index += 1

	fd.close()

def is_str_number(str):
    if (str.split(".")[0]).isdigit() or str.isdigit() or  (str.split('-')[-1]).split(".")[-1].isdigit():
        return True
    else:
        return False

def remove_illegal_char(input):
	out = ""

	illegal_char_set = ("%", "(", ")", "/", " ")

	for c in input:
		if c in illegal_char_set :
			if c == "%":
				out += "p"
			continue
		else:
			out += c
	return out

table_columns_name = NULL
value_format = NULL
def trans_line2Createsql(line):
	global table_columns_name
	global value_format

	sql = ""
	table_columns_name = ""
	value_format = ""

	columns = line.split("\t")
	idx = 0
	for column in columns:
		if len(column) > 1 :
			if idx != 0:
				sql += ","
				table_columns_name += ','
				value_format += ','

			column = remove_illegal_char(column)		
			sql += column
			#sql += " VARCHAR(50)"
			#sql += " TINYBLOB"
			sql += " text"
			
			table_columns_name = table_columns_name + '\'' + column + '\''
			value_format += '%s'

			if idx == 0:
				sql += " NOT NULL"
			idx += 1
	return sql

def trans_line2Insertsql(line):
	line = line.replace('\t', "\",\"")
	line = "\"" + line[:-3]

	sql = ""
	for c in line:
		if c != " ":
			sql += c
			
	return sql

def create_table(tbl_name, file_name, cursor):
	create_sql = "CREATE TABLE"
	create_sql = create_sql + " " + tbl_name + " ("

	fd = open(file=file_name, mode='r')
	line = fd.readline()
	fd.close()

	line = trans_line2Createsql(line)
	
	create_sql += line
	create_sql += ")" 
	create_sql += "ENGINE=InnoDB DEFAULT CHARSET=utf8"

	drop_table_if_exists = "DROP TABLE IF EXISTS " + tbl_name
	cursor.execute(drop_table_if_exists)
	cursor.execute(create_sql)

def insert_data(tbl_name, file_name, cursor):
	global table_columns_name

	insert_sql_base = "INSERT INTO"
	#insert_sql_base += " " + tbl_name + "(" + table_columns_name + ") " + "VALUES " + " (" + value_format + ')' + ' % ' + '('
	#insert_sql_base += " " + tbl_name + "(" + table_columns_name + ") " + "VALUES " + " ("
	insert_sql_base += " " + tbl_name + " VALUES " + " ("

	fd = open(file=file_name, mode='r')
	fd.readline()

	while 1:
		line = fd.readline()
		if not line:
			break
		else:
			line = trans_line2Insertsql(line)
			
			insert_sql = insert_sql_base + line + ")"
			if 0:
				print(insert_sql)
			else:
				cursor.execute(insert_sql)
				db.commit()

			'''
			try :
				cursor.execute(insert_sql)
				db.commit()
			except:
				db.rollback()
			'''
			#break
	fd.close()

# (hsag_file, hsag) hsag mains 沪深A股
def import_file_data(file_name, tbl):
	connect_db()

	create_table(tbl, file_name, cursor)
	insert_data(tbl, file_name, cursor)

	disconnect_db()

# select  column_name from information_schema.columns where table_schema ='tdx'  and table_name = 'hsag'
def get_columns_name(schema, tbl):
	connect_db()
	sql = "select  column_name from information_schema.columns where table_schema =" + "\'" + schema + "\'" + " and table_name = " + "\'" + tbl + "\'"
	#print(sql)
	column_num = cursor.execute(sql)
	columns = cursor.fetchall()
	#print(column_num)
	#print(res)
	disconnect_db()

	return columns

# ('tdx', 'hsag') 
def average_pe(schema, tbl):
	columns = get_columns_name(schema, tbl)
	
	sel_sql = "select " + columns[4][0] + "," + columns[5][0] +  " from "+ schema + '.' + tbl  #+ " where " + columns[4][0] +" > 0"
	connect_db()
	print(sel_sql)
	cursor.execute(sel_sql)
	res = cursor.fetchall()

	sum = 0
	cnt = 0
	minus_cnt = 0
	for row in res:	
		if is_str_number(row[1][:-2]) and is_str_number(row[0]):
			sum += float(row[1][:-2])
			cnt += 1
		else:
			minus_cnt += 1
			#print(row[0], row[1])
	print(cnt, minus_cnt, round(sum, 2))
	
	ave = 0
	ave_cnt = 0
	for row in res:
		if is_str_number(row[1][:-2]) and is_str_number(row[0]):
			ave += float(row[0]) * float(row[1][:-2]) / sum	
			ave_cnt += 1
	print(ave_cnt, round(ave,2))

	disconnect_db()

def test_1_hsag():
	import_file_data(hsag_file, 'hsag')
	average_pe('tdx', 'hsag')

def test_1_shanghai_main():
	import_file_data(shanghai_main_file, 'shanghai_main')
	average_pe('tdx', 'shanghai_main')

def test_1_shenzheng_main():
	import_file_data(shenzheng_main_file, 'shenzheng_main')
	average_pe('tdx', 'shenzheng_main')

def test_1_cyb():
	import_file_data(cyb_file, 'cyb')
	average_pe('tdx', 'cyb')

def test_1_kcb():
	import_file_data(kcb_file, 'kcb')
	average_pe('tdx', 'kcb')

if __name__ == "__main__":
	average_pe('tdx', 'hsag')
	average_pe('tdx', 'shanghai_main')
	average_pe('tdx', 'shenzheng_main')
	average_pe('tdx', 'cyb')
	average_pe('tdx', 'kcb')
	#test_1_hsag()
	#test_1_shanghai_main()
	#test_1_shenzheng_main()
	#test_1_cyb()
	#test_1_kcb()