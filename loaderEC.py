#Authors: Cindy Chao, Dehowe Feng
#Loads values into postgresql from H5
#Python Version: 2.7.1

import os, psycopg2, sys, h5py, numpy as np, matplotlib.pyplot as plt

def isREAL(txt):
	try:
		float(txt)
		return True
	except  ValueError:
		return False

###########################
#   Builds NTHS table     #
###########################
def buildNTHS(groupname):
	
	if 'PER' in groupname:
		tablename = 'PERSON_H5'
	elif 'VEH' in groupname:
		tablename = 'VEHICLE_H5'
	elif 'DAY' in groupname:
		tablename = 'DAY_H5'
	elif 'HHV2' in groupname:
		tablename = 'HOUSEHOLD_H5'
	else:
		print "Error, what are you trying to read??"
		sys.exit()

	print "Creating table", tablename, "from HW4Data.mat in group",groupname,"..."

	f = h5py.File('HW4Data.mat', 'r')
	group = f[groupname]
		
	#Builds the Table
	sql = "CREATE TABLE IF NOT EXISTS " + tablename + "\n(\n"	
	sql += ' FLOAT(8),\n'.join(str(item) for item in group)
        sql += ' FLOAT(8));'
	#print sql
	cur.execute(sql)
	
	#Loads the Table
	rowcount = len(f[groupname].values()[0])
	 #need count to keep track of tuples since h5 files are trees
	colcount = len(group)
	x = 1 #count for inserting by 1000
	insertcount = 1
	for row in range(0, rowcount):

		if(x == 1):
			sql = "INSERT INTO " + tablename + " VALUES "
	
		count = 0	
		for col in group:
			if count == 0:
				sql += "("
			if isREAL(f[groupname][col][row]):
				sql += str(f[groupname][col][row])+ ', '
			else:
				sql += '66666' + ', '

			if count == colcount - 1:
				sql = sql[:-2]
				sql += ")"
			count +=1
		insertcount += 1
		if(x != 1000):
			sql += ", \n"

		if(x == 1000):
			sql += "\n ;"
			sql = str(sql)
			print insertcount, "insertions made."
			cur.execute(sql)
			sql = ""
			x = 1
			continue
		x += 1
	if sql != "":
		sql = str(sql)[:-3] + ';'
		print sql
		cur.execute(sql)
		print insertcount, "insertions made."	


###########################
#   Builds EIA  table     #
###########################
def buildEIA(groupname):

	if 'ELEC_CO2' in groupname:
		tablename = 'ELECTRICITY_H5'
	elif 'TRANS' in groupname:
		tablename = 'TRANSPORTATION_H5'
	elif 'MKWH' in groupname:
		tablename = 'MKWH_H5'
	elif 'Strings' in groupname:
		tablename = 'Strings'
	else:
		print "Error, what are you trying to read??"
		sys.exit()

	print "Creating table", tablename, "from HW4Data.mat in group",groupname,"..."

	f = h5py.File('HW4Data.mat', 'r')
	group = f[groupname]
		

	sql = "CREATE TABLE IF NOT EXISTS " + tablename + "\n(\n"	
	sql += ' VARCHAR(100),\n'.join(str(item) for item in group)
        sql += ' VARCHAR(100));'
#	print sql
	cur.execute(sql)

	#Loads the Table
	rowcount = len(f[groupname].values()[0])
	 #need count to keep track of tuples since h5 files are trees
	colcount = len(group)
	x = 1 #count for inserting by 1000
	insertcount = 1
	for row in range(0, rowcount):

		if(x == 1):
			
			sql = "BEGIN TRANSACTION INSERT INTO " + tablename + " VALUES "
	
		count = 0	
		for col in group:
			if count == 0:
				sql += "("
			sql += '\'' + str(f[groupname][col][row]) +'\', '

			if count == colcount - 1:
				sql = sql[:-2]
				sql += ")"
			count +=1
		insertcount += 1
		if(x != 1000):
			sql += ", \n"

		if(x == 1000):
			sql += "\n ; END TRANSACTION"
			sql = str(sql)
			cur.execute(sql)
			print insertcount, "insertions made."
			sql = ""
			x = 1
			continue
		x += 1
	if sql != "":
		sql = str(sql)[:-3] + ';'
		#print sql
		cur.execute(sql)
		print insertcount, "insertions made."	

#open connection
try:
	conn = psycopg2.connect(database="postgres", host="/home/" + os.environ['USER'] + "/postgres")
	cur = conn.cursor()
	
	print "Connection Success"
except:
	print "Connection Unsuccessful"

buildNTHS("PERV2PUB")
buildNTHS("VEHV2PUB")
buildNTHS("DAYV2PUB")
buildNTHS("HHV2PUB")
buildEIA("ELEC_CO2")
buildEIA("TRANS_CO2")
buildEIA("ELEC_MKWH")
buildEIA("Strings")
conn.commit()
cur.close()
conn.close()
