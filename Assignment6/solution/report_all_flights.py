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

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full):
    print("Flight %s (%s):"%(flight_id,airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time,arrival_time,duration_minutes))
    print("    %s -> %s"%(source_airport_name,dest_airport_name))
    print("    %s (%s): %s/%s seats booked"%(aircraft_id, aircraft_model,seats_full,seating_capacity))

cursor.execute("""select flight_id, airline, source_airport_name, dest_airport_name, departure as departure_time, arrival as arrival_time, (extract(epoch from departure) - extract(epoch from arrival))/60 as duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full from flights natural join
(select airport_name as source_airport_name, flights.source_iata_code from airports inner join flights on airports.iata_code = flights.source_iata_code) as T1 natural join
(select airport_name as dest_airport_name, flights.destination_iata_code from airports inner join flights on airports.iata_code = flights.destination_iata_code) as T2 natural join
(select count(*) as seats_full, flight_id from reservations group by flight_id) as T3 natural join
(select model as aircraft_model, passenger_capacity as seating_capacity, flights.aircraft_id from aircrafts inner join flights on aircrafts.aircraft_id = flights.aircraft_id) as T4
order by departure;""" )

while True:
    row = cursor.fetchone()
    if row is None:
        break
    print_entry(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])

conn.commit()
cursor.close()
conn.close()	
