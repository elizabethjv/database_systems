# report_all_flights.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/29/2020

import psycopg2, sys

psql_user = 'elizabethv'  #Change this to your username
psql_db = 'elizabethv' #Change this to your personal DB name
psql_password = 'V00883616' #Put your password (as a string) here
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

def print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity):
    print("%-5s (%s): %s"%(aircraft_id, model_name, airline))
    print("    Number of flights : %d"%num_flights)
    print("    Total flight hours: %d"%flight_hours)
    print("    Average passengers: (%.2f/%d)"%(avg_seats_full,seating_capacity))

cursor.execute("""select aircraft_id, airline, model as model_name, num_flights, flight_hours, avg_seats_full, passenger_capacity as seating_capacity from aircrafts natural join reservations  natural join
        (select count(*) as num_flights, aircraft_id from flights natural join aircrafts group by aircraft_id) as T1 natural join
        (select round(sum((extract(epoch from departure) - extract(epoch from arrival))/60*60)) as flight_hours, aircraft_id from flights group by aircraft_id) as T2 natural join
        (select num_of_reservations/ num_of_flights as avg_seats_full, aircraft_id from (select count(*) as num_of_reservations, aircraft_id from reservations natural join flights group by aircraft_id) as T4 natural join (select count(*) as num_of_flights, aircraft_id from flights group by aircraft_id) as T5) as T3
        order by aircraft_id;""" )

while True:
    row = cursor.fetchone()
    if row is None:
        break 
    print_entry(row[0], row[1], row[2], row[3], row[4], row[5], row[6])
    
conn.commit()
cursor.close()
conn.close()
