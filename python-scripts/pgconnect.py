# Scott Newby - 2022-05-06
# Define connection routines for various databases
# Expand as necessary
# PostgreSQL connection script

# imports

import psycopg2

def pgconnect(db,usr,pwd,host,port):
    print('pgconnect')
    #define the postgresql database connection
    # TODO - try catch
    pgconnection = psycopg2.connect(user=usr, password=pwd,host=host, port=port,database=db)
    
    return pgconnection

