#!/usr/bin/python3

import pymysql

def test_pymysql():
	db = pymysql.connect(host="localhost", port=3306, user="root", passwd="admin", db="tdx")
	cursor = db.cursor()
	cursor.execute("SELECT VERSION()")

	data=cursor.fetchone()
	print("Database version: %s" % data)#
	db.close()
	print("hello")

if __name__ == '__main__':
	test_pymysql()