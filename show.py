#! /usr/bin/env python
# coding: utf-8

import argparse, sys
from resv import sqliteBase, decode

# コマンドライン引数用
def cmdline_parse():
	parser = argparse.ArgumentParser(
		prog = "python show.py",
		formatter_class = argparse.RawDescriptionHelpFormatter
	)

	parser.add_argument(
		"-t","--target",
		type=str,
		default="all",
		help="ホスト名を入力してください"
	)

	parser.add_argument(
		"-s","--select",
		type=str,
		default=False,
		metavar="\"QUERY\"",
		help="SQL:select文を入力してください"
	)

	parser.add_argument(
		"-a","--all",
		action="store_true",
		help="すべてのログを表示します"
	)

	args = parser.parse_args()

	return args

class analize(sqliteBase,object):
	def query(self,query):
		data = self.select(decode(query))

		for record in data:
			print("{0}\t{1}".format(record[0],record[1]))

	def select_all(self):
		data = self.select("select * from {0}".format(self.table))

		print("{0}\t{1}\t{2}\t{3}\t{4}"
			.format("HOST","DATE","METHOD","REQUEST","STAUS"))

		for i in data:
			print("{0}\t{1}\t{2}\t{3}\t{4}"
				.format(i[0],i[1],i[2],i[3],i[4]))

def main():
	args = cmdline_parse()
	cmd = analize()

	if args.select:
		cmd.query(args.select)
	elif args.all:
		cmd.select_all()

if __name__ == "__main__":
	main()
