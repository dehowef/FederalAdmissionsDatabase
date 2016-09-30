import csv
import os
import psycopg2

conn = psycopg2.connect(database="postgres", host= "/home/" + os.environ['USER'] + "/postgres")
print "Opened db successfully"

cur = conn.cursor();
##############
#  QUERY 3A  #
##############

#months of survey
months = [200803,200804,200805,200806,200807,200808,200809,200810,200811,200812,200901,200902,200903,200904]

#get number of days in month to calculate number of individuals making a trip
def days_in_month(month):
	if (month == "04" or month == "06" or month == "09" or month == "11"):
		days = 30
	elif (month == "02"):
		days = 28
	else:
		days = 31
	return days

##############
#  QUERY 3A  #
##############
def query3A(): 
	#get the traveldaydate of all individuals who travel less than 100 miles a day
	query_total = "SELECT TDAYDATE FROM (SELECT SUM(TRPMILES) AS miles, HOUSEID, PERSONID, TDAYDATE FROM DAY GROUP BY HOUSEID,PERSONID,TDAYDATE) Trip WHERE Trip.miles < 100"
	cur.execute(query_total);
	totalnum = cur.fetchall()

	#calculate the total number of individuals who travel less than 100 miles a day
	individuals_less_than_100 = 0
	for num in totalnum:
		month = str(num[0])[4:6]
		individuals_less_than_100 += days_in_month(month) #will hold the total num of individuals who travel less than 100 mpd at the end of loop
	#print individuals_less_than_100
	
	#get the individuals who travel less than X miles
	query = "SELECT TDAYDATE, miles FROM (SELECT SUM(TRPMILES) AS miles,HOUSEID,PERSONID,TDAYDATE FROM DAY GROUP BY HOUSEID,PERSONID,TDAYDATE) Trip WHERE Trip.miles < "
	
	print "QUERY 3A"
	mile = 5
	while (mile <= 100):
		cur.execute(query + str(mile))
		result = cur.fetchall() #the TDAYDATE and miles of everyone who travels less than X miles. each tuple represents ONE individual
		num_people = 0
		for person in result:
			month = str(person[0])[4:6] #parsing out the month in TDAYDATE
			num_people += days_in_month(month) #loop until get the total num of individuals, assuming they travel the same num of miles each day of that month
		#print str(num_people) + " travel less than " + str(mile) + " miles a day"
		print str("%.2f" % (num_people/float(individuals_less_than_100)*100)) + "% of people travel less than " + str(mile) + " miles a day"	
		mile += 5
	print " "

##############
#  QUERY 3B  #
##############
def query3B():
	print "QUERY 3B"
	for i in range (5, 101, 5):
		sql =  "SELECT AVG(EPATMPG) FROM VEHICLE INNER JOIN DAY ON DAY.HOUSEID" 
		sql += "= VEHICLE.HOUSEID WHERE VEHICLE.VEHID >= 1 AND DAY.TRPMILES < %d;" % i
		#print sql
		cur.execute(sql)
		print "Average EPATMPG for trips less than",i,"is", cur.fetchone()[0]
	print " " 
##############
#  QUERY 3C  #
##############
def query3C():
	#get the total amount of c02 emissions from every household in month X 
	sum_subquery = "SELECT SUM((1.0 * TRPMILES)/(1.0*EPATMPG)*.008883) FROM (SELECT * FROM DAY NATURAL JOIN VEHICLE WHERE VEHID >=1 AND TRPMILES > 0 AND TDAYDATE ="

	#get the total amount of c02 emissions of transportation sector for a specific month
	total_c02_subquery = "SELECT VALUE FROM TRANSPORTATION WHERE MSN = 'TEACEUS' AND YYYYMM ="

	#get number of households surveyed in month X
	num_house_subquery = "SELECT COUNT(DISTINCT(HOUSEID)) FROM DAY WHERE VEHID >= 1 AND TRPMILES > 0 AND TDAYDATE ="
	
	print "QUERY 3C"
	for month in months:
		#sum of c02 emissions
		cur.execute(sum_subquery + str(month) + ") AS day_join_v")
		month_sum_c02 = cur.fetchall()

		#num of households
		cur.execute(num_house_subquery + str(month))
		households = cur.fetchall()

		#total c02 emissions from EIA data
		cur.execute(total_c02_subquery + "'" + str(month) + "'")
		c02 = cur.fetchall()

		#extract from tuples
		month_co2_emission = month_sum_c02[0][0]*days_in_month(month)
		num_households = households[0][0]
		total_c02 = c02[0][0]

		house_scale = 117538000/num_households
		scaled_co2_emission = (month_co2_emission * house_scale)/(float(total_c02)*1000000) * 100
	
		print "For " + str(month) + ", the percentage of c02 emissions from household vehicles: " + str("%.2f" % scaled_co2_emission) + "%"
	print " "

#############
#  QUERY 3D #
#############
def query3D():
	print "QUERY 3D"
	mile_range = [20.0, 40.0, 60.0]
	for mile in mile_range:
		for month in months:
			#get number of households surveyed in month X
			num_house_subquery = "SELECT COUNT(DISTINCT(HOUSEID)) FROM DAY WHERE VEHID >= 1 AND TRPMILES > 0 AND TDAYDATE =" 
			cur.execute(num_house_subquery + str(month))
			households = cur.fetchall()
			num_households = households[0][0]
			#print "number houses: " + str(num_households)
			house_scale = float(117538000/num_households)

			#get the total amount of c02 emissions from gasoline from vehicles for one day in month X (don't multiply by num of days yet)
			gasoline_c02_query = "SELECT SUM(((1.0*TRPMILES)/EPATMPG)*.008887) FROM DAY NATURAL JOIN VEHICLE WHERE VEHID >=1 AND TRPMILES > 0 AND TDAYDATE ="
			cur.execute(gasoline_c02_query + str(month))
			gasoline_c02 = cur.fetchall()

			total_c02 = gasoline_c02[0][0]*31*house_scale
			#print "total c02 before scale: " + str(gasoline_c02[0][0]*31)
			#print "total c02 from normal vehicles: " + str(total_c02)

			#get conversion for KWH -> CO2
			c02_ratio_query = "SELECT ELECTRICITY.VALUE, MKWH.VALUE FROM ELECTRICITY,MKWH WHERE MKWH.MSN='ELETPUS' AND ELECTRICITY.MSN='TXEIEUS' AND ELECTRICITY.YYYYMM = "
			cur.execute(c02_ratio_query + "'" + str(month) + "'" + " AND MKWH.YYYYMM='" + str(month) + "'")
			c02_ratio = cur.fetchall()
			ratio = float(c02_ratio[0][0])/float(c02_ratio[0][1])

			#get total kwh from hybrids who only drive in their electricity range
			hybrids_drive_20_query = "SELECT SUM(miles/(EPATMPG*0.09063441)) FROM (SELECT SUM(TRPMILES) as miles,EPATMPG FROM DAY NATURAL JOIN VEHICLE WHERE VEHID>=1 AND TRPMILES>0 AND TDAYDATE="
			cur.execute(hybrids_drive_20_query + str(month) + " GROUP BY HOUSEID,VEHID,EPATMPG)sub WHERE miles <=" + str(mile))
			sum_result = cur.fetchall()
			hybrids_20 = sum_result[0][0]*31*house_scale*ratio
			#print "c02 from vehicles who only drive electric: " + str(hybrids_20)
	
			#get total kwh and gas used from hybrids who drive more than their electricity range
			hybrids_drive_more_query = "SELECT SUM(" + str(mile) + "/(EPATMPG*0.09063441)), SUM(((totalmiles-" + str(mile) + ")/EPATMPG)*.008887) FROM (SELECT SUM(TRPMILES) totalmiles, EPATMPG FROM DAY NATURAL JOIN VEHICLE WHERE VEHID>=1 AND TRPMILES>0 AND TDAYDATE= " 
			cur.execute(hybrids_drive_more_query + str(month) + " GROUP BY HOUSEID,VEHID,EPATMPG)trip WHERE totalmiles >" + str(mile))
			kwh_gas_result = cur.fetchall()

			kwh_electricity = kwh_gas_result[0][0]*31*house_scale*ratio
			#print "c02 from first 20 miles: " + str(kwh_electricity)
			c02_gas = kwh_gas_result[0][1]*31*house_scale
			#print "c02 from further miles: " + str(c02_gas)
			hybrids_drive_more_20 = kwh_electricity+c02_gas
			#print "total c02 from those miles: " + str(hybrids_drive_more_20)

			#get total C02 emissions from electricity
			c02_from_hybrids = hybrids_20 + hybrids_drive_more_20
			#print "total c02 from hybrids: " + str(c02_from_hybrids)
			#print "total c02 from hybrids before scale: " + str((gasoline_c02[0][0] + kwh_gas_result[0][0] + kwh_gas_result[0][1]))
			# subtract electricity + remaining co2 emissions using gas from total co2 emissions using 100% gasoline
			change_in_c02 = total_c02 - c02_from_hybrids
			print "For " + str(month) + " change in c02 if all vehicles were hybrids with electric range of " + str(mile) +" miles: " + str(change_in_c02)
	print " "
query3A()
query3B()	
query3C()
query3D()
conn.close()
