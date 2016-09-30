#Authors: Cindy Chao, Dehowe Feng
#Loads values into postgresql
#Python Version: 2.7.1

import os
import psycopg2
import csv
import sys

def isREAL(txt):
	try:
		float(txt)
		return True
	except  ValueError:
		return False

###########################
#   Builds NTHS table     #
###########################
def buildNTHS(filename):
	
	if 'PER' in filename:
		tablename = 'PERSON'
	elif 'VEH' in filename:
		tablename = 'VEHICLE'
	elif 'DAY' in filename:
		tablename = 'DAY'
	elif 'HHV2' in filename:
		tablename = 'HOUSEHOLD'
	else:
		print "Error, what are you trying to read??"
		sys.exit()

	print "Creating table", tablename, "from", filename, "..."

	with (open(filename)) as f:
		reader = csv.reader(f)
		row = reader.next()
		sql = "CREATE TABLE IF NOT EXISTS " + tablename + "\n(\n" 
		for value in row[:-1]:
			sql += value + ' FLOAT(8), \n'
		sql += row[-1] + ' FLOAT(8)\n); '
		sql = str(sql)
		cur.execute(sql)
	loadNTHS(tablename, filename) #end buildNTHS


###########################
#   Builds EIA  table     #
###########################
def buildEIA(filename):

	if 'Electricity' in filename:
		tablename = 'ELECTRICITY'
	elif 'Transportation' in filename:
		tablename = 'TRANSPORTATION'
	elif 'MkWh' in filename:
		tablename = 'MKWH'
	else:
		print "Error, what are you trying to read??"
		sys.exit()

	print "Creating table", tablename, "from", filename, "..."

	with (open(filename)) as f:
		reader = csv.reader(f)
		row = reader.next()
		
		sql = "CREATE TABLE IF NOT EXISTS " + tablename + "\n(\n" 
		for value in row[:-1]:
			sql += value + ' VARCHAR(100), \n'
		sql += row[-1] + ' VARCHAR(100)\n); '
		sql = str(sql)
		cur.execute(sql)

	loadEIA(tablename, filename) #end buildEIA

###########################
#   Loads  NTHS table     #
###########################
#load table function
def loadNTHS(tablename, filename):
	print "Now loading table ", tablename, "..."
	with (open(filename, "rb")) as f:
		reader = csv.reader(f)
		row = reader.next()
		x = 1
		insertcount = 1
		for row in reader:
			if(x == 1):
				sql = "INSERT INTO " + tablename + " VALUES "
			sql += "(" + row[0] + ', '

			for value in row[1:-1]:
				if isREAL(value):	
					sql += value + ', '
				else:
					sql += "66666" + ', '
			
			if isREAL(row[-1]):
				sql += row[-1] + ")"
			else:
				sql += "66666" + ")"
			
			insertcount += 1	
			if(x != 1000):
				sql += ", \n"

			if(x == 1000):
				sql += "\n ;"
				sql = str(sql) #need a cast.
				cur.execute(sql)
				sql = "" 
				x = 1
				continue
			x += 1
		if sql != "": 
			sql = str(sql)[:-3] + "\n;"
			cur.execute(sql)
			print  insertcount, "insertions made."
			#insert here.


###########################
#   Loads  EIA  table     #
###########################
def loadEIA(tablename, filename):
	print "Now loading table ", tablename, "..."
	with (open(filename, "rb")) as f:
		reader = csv.reader(f)
		row = reader.next()
		x = 1
		insertcount = 1
		for row in reader:
			if(x == 1):
				sql = "INSERT INTO " + tablename + " VALUES "
			sql += "(\'" + row[0] + '\', '

			for value in row[1:-1]:
				sql += '\'' + value + '\', '
				
			sql += "\'" + row[-1] + "\')"
			
			insertcount += 1
			if(x != 1000):
				sql += ", \n"

			if(x == 1000):
				sql += "\n ;"
				sql = str(sql) #need a cast.
				cur.execute(sql)
				sql = "" 
				x = 1
				continue
			x += 1

		if sql != "": 
			sql = str(sql)[:-3] + ";"
			cur.execute(sql)
			print insertcount, "insertions made."

#open connection
try:
	conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
	cur = conn.cursor()
	
	print "Connection Success"
except:
	print "Connection Unsuccessful"

buildNTHS("PERV2PUB.CSV")
buildNTHS("VEHV2PUB.CSV")
buildNTHS("DAYV2PUB.CSV")
buildNTHS("HHV2PUB.CSV")
buildEIA("EIA_CO2_Electricity_2015.csv")
buildEIA("EIA_CO2_Transportation_2015.csv")
buildEIA("EIA_MkWh_2015.csv")
conn.commit()
cur.close()
conn.close()
