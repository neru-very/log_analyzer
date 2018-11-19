#! /usr/bin/env python
# coding: utf-8

import sys,os,re,glob
from datetime import datetime,timedelta
import sqlite3,argparse

if sys.version_info[0] == 2:
	import urllib as url
else:
	import urllib.parse as url

#########################################################################################
### Please input your information #######################################################
#########################################################################################
### file_path = "/var/log/http/access_log*"
file_path = "./log_data/access_log*"
server_format = "access_log[_]{,1}([^-]*)[-]{,1}[\d]*"
###
### access_logの正規表現
### リモートホスト名
h = "(\S*)" # 1
### クライアントの識別子
l = "(\S*)" # 1
### 認証ユーザー名
u = "(\S*)" # 1
### 時刻
t = "\[(\d{1,2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2}) \S*\]" # 1
### リクエストの最初の行の値
r = "(\S*) (\S*) (\S*)" # 3
### レスポンスステータス
s = "(\d{3})" # 1
### 送信されたバイト数,0バイトの時は-
b = "([-\d]*)" # 1
### Referer
referer = "([^\" ]*)" # 1
### User-Agent
user_agent = "([^\"]*)" # 1
### X-Forwarded-For
x = "([\S]*)" # 1
###
### LogFormat "%h %l %u %t \"%r\" %>s %b \"%{Referer}i\" \"%{User-Agent}i\"" combined
###            1   2   3   4    5-7    8   9        10             11
log_format = "{h} {l} {u} {t} \"{r}\" {s} {b} \"{referer}\" \"{user_agent}\""\
.format(h=h,l=l,u=u,t=t,r=r,s=s,b=b,referer=referer,user_agent=user_agent,x=x)
###
### データベースのカラム
columns = ["date","client","method","request","status","referer","user_agent"]
### columnsがログのどの位置か
log_index = [4,1,5,6,8,10,11]
###
### ログの時間フォーマット
time_format = "%d/%b/%Y:%H:%M:%S"
### 表示用の時間フォーマット
print_time = "%Y/%m/%d %H:%M:%S"
#########################################################################################
#########################################################################################
#########################################################################################

# コマンドライン引数
def arg():
	parser = argparse.ArgumentParser(
		prog = "python analyzer.py",
		formatter_class = argparse.RawDescriptionHelpFormatter,
		description="""
------------------------------------------------------------------------------------
ファイルの読み込み，データベースの更新
python analyzer.py --update

標準解析モード
python analyzer.py [--col COLUMN] [--since MINUTE] [--limit NUMBER]

データベース検索モード
python analyzer.py --search KEYWORD [--col COLUMN] [--since MINUTE]

時間解析モード
python analyzer.py --time [--interval MINUTE] [--since MINUTE]

＊デフォルト
COLUMN = date , client , method , request , status , referer , user_agent
------------------------------------------------------------------------------------
""",
		epilog="""
------------------------------------------------------------------------------------
"""
	)

	parser.add_argument(
		"--update",
		action="store_true",
		help="データベースの更新"
	)

	parser.add_argument(
		"--search",
		type=str,
		default=False,
		metavar="KEYWORD",
		help="検索モード"
	)

	parser.add_argument(
		"--time",
		action="store_true",
		help="時間出力モード"
	)

	parser.add_argument(
		"--since",
		type=int,
		default=60,
		metavar="MINUTE",
		help="指定時間前まで検索(default:60)"
	)

	parser.add_argument(
		"--col",
		type=str,
		default="request",
		metavar="COLUMN",
		choices=columns,
		help="カラム名を選択(default:request)"
	)

	parser.add_argument(
		"--limit",
		type=int,
		default=10,
		metavar="NUMBER",
		help="最大表示件数を指定(default:10)"
	)

	parser.add_argument(
		"--interval",
		type=int,
		default=10,
		metavar="MINUTE",
		help="時間間隔を指定(default:10)"
	)

	return parser.parse_args()

# str => time　に変換
def change_to_time(date):
	try:
		return datetime.strptime(date,time_format)
	except ValueError:
		return datetime.strptime(date,print_time)

# time => strに変換
def change_to_str(time):
	return time.strftime(print_time)

# columnはcolumnsの何番目か
def index(column):
	return columns.index(column)

# utf-8 => unicode
def decode(string):
	if isinstance(string,str):
		return string.decode("utf-8")

# URL encoding => utf-8
def url_decode(string):
	return url.unquote(string)

# nowのbefore分前の時間を取得
def get_time(now,before):
	return now + timedelta(minutes = -before)

# データベース操作用
class sqliteBase(object):
	# データベースへの接続
	def __init__(self):
		self.connect = sqlite3.connect("http_log.db")
		self.cursor = self.connect.cursor()

		self.tables = self.table_list()

	# insert または update
	# query = insert into {table}(col 1,col 2) values(?,?)
	# Tuple = (value 1,value 2)
	def insert(self,query,values=()):
		self.cursor.execute(query,values)
		self.connect.commit()

	# select または drop または create
	def select(self,query,values=()):
		self.cursor.execute(query,values)
		data = self.cursor.fetchall()

		return data

	# テーブルの一覧を取得
	def table_list(self):
		query = "select name from sqlite_master where type='table'"
		tables = self.select(query)

		return [table[0] for table in tables]

class sqlite(sqliteBase):
	# テーブルの作成
	def create_table(self,server):
		self.server = server
		# テーブルが存在しなければ作成
		if server in self.tables:
			return

		query = "create table {0}(".format(server)

		for col in columns:
			query += "{0} TEXT not NULL,".format(col)
		else:
			query = query[:-1] + ")"

		self.select(query)
		self.tables.append(server)

	# ログの挿入
	def insert_log(self,values):
		arg = "?," * len(values)
		query = "insert into {0} values({1})".format(self.server,arg[:-1])

		self.insert(query,values)

	#　最新のレコードを取得
	def get_last_time(self,server):
		query = "select date from {0} order by date desc limit 1".format(server)
		date = self.select(query)

		if date == []:
			return False

		return change_to_time(date[0][0])

	#　最古のレコードを取得
	def get_first_time(self,server):
		query = "select date from {0} order by date limit 1".format(server)
		date = self.select(query)[0][0]

		return change_to_time(date)

	# infomation
	def info(self):
		for table in self.tables:
			query = "select count(*) from {0}".format(table)
			count = self.select(query)[0][0]

			print("\t{0:<10} has {1} logs".format(table,str(count)))

	# 集計
	def show_count(self,column,since,limit):
		for table in self.tables:
			print("\033[35m# {0}\033[0m".format(table))
			print("{0:<8} |  {1}".format("COUNT",column.upper()))
			print("{0:-<64}".format(""))

			query = "select count({0}) as c,{0} from {1} where ? <= date group by {0} order by c limit {2}"\
			.format(column,table,limit)

			results = self.select(query,[since])

			for result in results:
				print("{0:<8} |  {1}".format(str(result[0]),result[1]))
				print("{0:-<64}".format(""))
			else:
				print("")

	# データベース検索
	def search(self,column,keyword,since):
		limit_time = change_to_str(get_time(datetime.now(),since))

		for table in self.tables:
			print("\033[35m# {0}\033[0m".format(table))
			print("{0:-<64}".format(""))

			query  = "select * from {0} where {1} like ? and ? <= date order by date desc"\
			.format(table,column)

			results = self.select(query,[u"%"+decode(keyword)+u"%",limit_time])

			for result in results:
				date = result[index("date")]
				client = result[index("client")]
				method = result[index("method")]
				status = result[index("status")]
				request = result[index("request")]
				referer = result[index("referer")]
				user_agent = result[index("user_agent")]

				print("{0}    From: {1}".format(date,client))
				print("*Request    : {0}  {1}  {2}".format(method,status,request))
				print("*Referer    : {0}".format(referer))
				print("*User-Agent : {0}".format(user_agent))

				print("{0:-<64}".format(""))
			else:
				print("")

	# 時間グラフの作成
	def time(self,interval,since):
		for table in self.tables:
			print("\033[35m# {0}\033[0m".format(table))
			print("{0:-<64}".format(""))

			to_time = datetime.now()
			limit_time = get_time(to_time,since)

			# type : datetime
			first_time = self.get_first_time(table)

			if limit_time <= first_time:
				limit_time = first_time

			# PRINT time FROM from_time TO to_time
			while limit_time < to_time:
				from_time = get_time(to_time,interval)

				To = change_to_str(to_time)
				From = change_to_str(from_time)

				query  = "select count(*) from {0} where ? <= date and date <= ?".format(table)

				count = self.select(query,[From,To])[0][0]
				print("{0} - {1} => {2}".format(From,To,str(count)))

				to_time = from_time

				print("{0:-<64}".format(""))

			print("")

# ログファイルの読み込み，データベースの更新
def update():
	sql = sqlite()

	files = sorted(glob.glob(file_path),key=lambda x:(-len(x),x))

	for File in files:
		filename = os.path.basename(File)
		server = re.search(server_format,filename).group(1)

		if server == "":
			server = "localhost"

		sql.create_table(server)

		with open(File,"r") as f:
			log_list = f.readlines()

		# type:datetime
		last_time = sql.get_last_time(server)

		count = 0

		for line in log_list:
			sys.stdout.write("\rNow Loading {0:<32} {1:<10} {2:>6} / {3:<6}"\
			.format(filename,server,str(count),str(len(log_list))))
			sys.stdout.flush()

			log = re.search(log_format,line)

			if log == None:
#				print line
				continue

			values = [log.group(i) for i in log_index]
			log_date = change_to_time(values[index("date")])

			if last_time:
				if log_date <= last_time:
					continue

			values[index("date")] = change_to_str(log_date)
			values[index("request")] = url_decode(values[index("request")])
			values[index("referer")] = url_decode(values[index("referer")])

			sql.insert_log(list(map(decode,values)))
			count += 1
		else:
			sys.stdout.write("\033[2K\rComplelte {0:<32} {1:<10} {2:>6} / {3:<6}\n"\
			.format(filename,server,str(count),str(len(log_list))))
			sys.stdout.flush()

	print("\nResult :")
	sql.info()

# main関数
if __name__ == "__main__":
	args = arg()
	sql = sqlite()

	print("")
	print("-"*64)

	if args.update:
		update()

	elif args.search:
		sql.search(args.col, args.search, args.since)

	elif args.time:
		sql.time(args.interval, args.since)

	else:
		sql.show_count(args.col, args.since, args.limit)

	print("-"*64)
