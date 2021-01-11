# manage_flights.py
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
        action = row[0]
        if action.upper() == 'DELETE':
            if len(row) != 2:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                #Maybe abort the active transaction and roll back at this point?
                break
            flight_id = row[1]
            #Handle the DELETE action here
            try:
                cursor.execute("delete from flights where flight_id = %s;", (flight_id,) )
            #conn.commit() #Only commit if no error occurs (commit will actually be prevented if an error occurs anyway)
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

        elif action.upper() in ('CREATE','UPDATE'):
            if len(row) != 8:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                #Maybe abort the active transaction and roll back at this point?
                break
            flight_id = row[1]
            airline = row[2]
            src,dest = row[3],row[4]
            departure, arrival = row[5],row[6]
            aircraft_id = row[7]
            #Handle the "CREATE" and "UPDATE" actions here
            if action.upper() == 'CREATE':
                try:
                    cursor.execute("insert into flights values(%s, %s, %s, %s, %s, %s, %s);",(flight_id, src, dest, aircraft_id, airline, departure, arrival) )
                #conn.commit() #Only commit if no error occurs (commit will actually be prevented if an error occurs anyway)
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

            if action.upper() == 'UPDATE':
                try:
                    cursor.execute("Update flights set flight_id = %s, source_iata_code = %s, destination_iata_code = %s, aircraft_id = %s, airline = %s, departure = %s, arrival = %s where flight_id = %s;",(flight_id, src, dest, aircraft_id, airline, departure, arrival, flight_id))
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
               


        else:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            #Maybe abort the active transaction and roll back at this point?
            break
        
conn.commit()
cursor.close()
conn.close()
