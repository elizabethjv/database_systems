# manage_reservations.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
#
#
# B. Bird - 06/28/2020

import sys, csv, psycopg2

if len(sys.argv) < 2:
    print("Usage: %s <input file>"%sys.argv[0],file=sys.stderr)
    sys.exit(1)
    
input_filename = sys.argv[1]

psql_user = 'elizabethv'  #Change this to your username
psql_db = 'elizabethv' #Change this to your personal DB name
psql_password = 'V00883616' #Put your password (as a string) here
psql_server = 'studdb2.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db,user=psql_user,password=psql_password,host=psql_server,port=psql_port)

cursor = conn.cursor()

with open(input_filename) as f:
    for row in csv.reader(f):
        if len(row) == 0:
            continue #Ignore blank rows
        if len(row) != 4:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            break
        action,flight_id,passenger_id,passenger_name = row
        
        if action.upper() not in ('CREATE','DELETE'):
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            break

        if action.upper() == 'CREATE':
            try:
                cursor.execute("insert into passengers values (%s, %s);", (passenger_id, passenger_name))
                conn.commit()
                cursor.execute("insert into reservations values (%s, %s);", (flight_id, passenger_id))
                conn.commit()
            except psycopg2.ProgrammingError as err: 
                #ProgrammingError is thrown when the database error is related to the format of the query (e.g. syntax error)
                print("Caught a ProgrammingError:",file=sys.stderr)
                print(err,file=sys.stderr)
                conn.rollback()
            except psycopg2.IntegrityError as err: 
                #IntegrityError occurs when a constraint (primary key, foreign key, check constraint or trigger constraint) is violated.
                print("Caught an IntegrityError:",file=sys.stderr)
                print(err,file=sys.stderr)
                conn.rollback()
            except psycopg2.InternalError as err:  
                #InternalError generally represents a legitimate connection error, but may occur in conjunction with user defined functions.
                #In particular, InternalError occurs if you attempt to continue using a cursor object after the transaction has been aborted.
                #(To reset the connection, run conn.rollback() and conn.reset(), then make a new cursor)
                print("Caught an IntegrityError:",file=sys.stderr)
                print(err,file=sys.stderr)
                conn.rollback()

        if action.upper() == 'DELETE':
            try:
                cursor.execute("delete from reservations where passenger_id = %s;", (passenger_id,))
                conn.commit()
            except psycopg2.ProgrammingError as err: 
                #ProgrammingError is thrown when the database error is related to the format of the query (e.g. syntax error)
                print("Caught a ProgrammingError:",file=sys.stderr)
                print(err,file=sys.stderr)
                conn.rollback()
            except psycopg2.IntegrityError as err: 
                #IntegrityError occurs when a constraint (primary key, foreign key, check constraint or trigger constraint) is violated.
                print("Caught an IntegrityError:",file=sys.stderr)
                print(err,file=sys.stderr)
                conn.rollback()
            except psycopg2.InternalError as err:  
                #InternalError generally represents a legitimate connection error, but may occur in conjunction with user defined functions.
                #In particular, InternalError occurs if you attempt to continue using a cursor object after the transaction has been aborted.
                #(To reset the connection, run conn.rollback() and conn.reset(), then make a new cursor)
                print("Caught an IntegrityError:",file=sys.stderr)
                print(err,file=sys.stderr)
                conn.rollback()
        
		#Do something with the data here
		#Make sure to catch any exceptions that occur and roll back the transaction if a database error occurs.


cursor.close()
conn.close()	
		
