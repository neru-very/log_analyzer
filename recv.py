#! /usr/bin/env python
# coding: utf-8

### Attention ###
### 高負荷時には取りこぼす可能性があります(socketで受け取れていない:オーバーフロー)
### > ab -n 10000 -c 100 http://192.168.1.10/cavcvasnackbj/acljbjbwqc
###   データベースのレコード数 6238

### Apache/2.4.6 (CentOS) (internal dummy connection)　はByte長が-になっているためエラー

import socket
import sqlite3
import re
import sys
from multiprocessing import Process, Queue

logo = """
 ###       #######  #######  ###   ######
 ###       #######  #######  ###  #######
 ###       ###        ###    ###  ###
 ###       #######    ###    ###  ######
 ###       #######    ###          ######
 ###       ###        ###             ###
 ########  #######    ###         #######
 ########  #######    ###         ######


 ###  ###    ####    #######   ######    ######  ####   ###  #######  ####   ###   ########
 ###  ###   ######   ########  #######   ######  ### #  ###  #######  ### #  ###  ##########
 ###  ###  ###  ###  ###  ###  ###  ###  ###     ### #  ###    ###    ### #  ###  ###
 ########  ###  ###  ###  ###  ###   ##  ######  ### #  ###    ###    ### #  ###  ###  #####
 ########  ########  ########  ###   ##  ######  ###  # ###    ###    ###  # ###  ###  #####
 ###  ###  ########  #######   ###  ###  ###     ###  # ###    ###    ###  # ###  ###    ###
 ###  ###  ###  ###  ###  ###  #######   ######  ###  # ###  #######  ###  # ###  ##########
 ###  ###  ###  ###  ###   ### ######    ######  ###   ####  #######  ###   ####   ########

"""

#########################################################################################
### Please input your information #######################################################
#########################################################################################
###
### { サーバ名(remote) : syslogを受け取るポート(local) }
hosts = { "teamA" : 20001 , "teamB" : 20002 , "teamC" : 20003 }
### hosts = { "teamA" : 20001 }
###
### syslogを受け取るインターフェースのIPアドレス
hostIP = ""
#########################################################################################
#########################################################################################
#########################################################################################

#########################################################################################
### Please input your information #######################################################
#########################################################################################
### syslogの正規表現
###
### CustomLog "|/usr/bin/logger -p local5.info -t httpd_access" combined
### syslogによるヘッダ  <174>Nov 14 15:19:37 pc05 httpd_access:
header = "<[0-9]{1,}>(.*) httpd_access:"
h = "(\S*)"
l = "(\S*)"
u = "(\S*)"
t = "\[(\d{1,2}/\w{3}/\d{4}):(\d{2}:\d{2}:\d{2}) (\S*)\]"
r = "(\S*) (\S*) (\S*)"
s = "(\d{3})"
b = "(\d*)"
Referer = "([^\" ]*)"
User_Agent = "([^\"]*)"
###
### LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
###
syslog_format = "{0} {1} {2} {3} {4} \"{5}\" {6} {7} \"{8}\" \"{9}\""\
.format(header,h,l,u,t,r,s,b,Referer,User_Agent)

### [client, date, time, method, request, status, referer, user-agant]
index = [2,5,6,8,9,11,13,14]
#########################################################################################
#########################################################################################
#########################################################################################

def encode(string):
	if isinstance(string,unicode):
		return string.encode("utf-8")
	else:
		return string

def decode(string):
	if isinstance(string,str):
		return string.decode("utf-8")
	else:
		return string

# データベース用関数
class sqliteBase(object):
	# データベースへの接続
	def __init__(self):
		self.connect = sqlite3.connect("syslog.db")
		self.cursor = self.connect.cursor()

		self.table = "syslog"

	# insert または update
	# query = insert into {table}(col 1,col 2) values(?,?)
	# Tuple = (value 1,value 2)
	def insert(self,query,values=()):
		try:
			self.cursor.execute(query,values)
			self.connect.commit()
		except Exception as error:
			self.connect.rollback()
			print(error)
			print("SQL Error : {0}".format(query))

	# select または drop または create
	def select(self,query,values=()):
		try:
			self.cursor.execute(query,values)
			data = self.cursor.fetchall()
		except Exception as error:
			print(error)
			print("SQL Error : {0}".format(query))

		return data

	# テーブルの一覧を取得
	def table_list(self):
		query = "select name from sqlite_master where type='table'"
		tables = self.select(query)

		return [table[0] for table in tables]

# データベースの初期化
class init_database(sqliteBase):
	def __init__(self):
		super(init_database,self).__init__()

		# テーブルが存在しなければ作成
		if self.table not in self.table_list():
			self.create_table()

	# テーブルの作成
	def create_table(self):
		query = "create table {0}(\
			host TEXT not NULL,\
			date TEXT not NULL,\
			time TEXT not NULL,\
			client TEXT not NULL,\
			method TEXT not NULL,\
			request TEXT not NULL,\
			status INTEGER not NULL,\
			referer TEXT not NULL,\
			user_agent TEXT not NULL\
		)".format(self.table).replace("\t","")

		self.select(query)

# データベースへ保存
class save(sqliteBase):
	def insert_log(self,queue):

		count = 1

		while True:
			data = queue.get()
			syslog = re.search(syslog_format,data[1])

			sys.stdout.write("\r --- {0} packet received".format(str(count)))
			count += 1

			if syslog == None:
				print("\n\033[45;37;1m error \033[0m : {0}".format(data))
				count -= 1
				continue

			values = [data[0]]

			for i in index:
				values.append(syslog.group(i))

			query = "insert into {0} values(?,?,?,?,?,?,?,?,?)".format(self.table)
			self.insert(query,tuple(values))

def receive(host,port,queue):
	soc = socket.socket(socket.AF_INET,socket.SOCK_DGRAM)
	soc.bind((hostIP,port))

	while True:
		data = soc.recv(2048)
		queue.put(["",data])

def main():
	print(logo)

	queue = Queue()

	jobs = [Process(target=receive,args=(host,port,queue)) for host,port in hosts.items()]

	init_db = init_database()
	del init_db

	for job in jobs:
		job.start()

	log = save()
	log.insert_log(queue)

	for job in jobs:
		job.join()

if __name__ == "__main__":
	main()
