import psycopg2
import pandas as pd
import sys
import os

BLUE, RED, WHITE, YELLOW, MAGENTA, GREEN, END = '\33[94m', '\033[91m', '\33[97m', '\33[93m', '\033[1;35m', '\033[1;32m', '\033[0m'

def updatedb(file_path, table_name, dbname, host, port, user, pwd):
    '''
    This function upload csv to a target table
    '''
    try:
        conn = psycopg2.connect(dbname=dbname, host=host, port=port,\
         user=user, password=pwd)
        print("Connecting to Database")
        cur = conn.cursor()

        with open(file_path, "r") as f:
            # Truncate the table first
            cur.execute("Truncate {} Cascade;".format(table_name))
            print("Truncated {}".format(table_name))

            # Load table from the file with header
            cur.copy_expert("copy {} from STDIN CSV HEADER QUOTE '\"'".format(table_name), f)
            cur.execute("commit;")

            print("Loaded data into {}".format(table_name))
            conn.close()
            print("DB connection closed.")

    except Exception as e:
        print("Error: {}".format(str(e)))
        sys.exit(1)

def addInfo(table_name, file_name):
    dbname = 'postgres'
    host = 'turntablexe.cpvjm9jzsnd3.us-east-2.rds.amazonaws.com'
    port = '5432'
    user = 'postgres'
    pwd = 'TOaBiobZCuqcKsKhmedo'
    updatedb(file_name, table_name, dbname, host, port, user, pwd)

if __name__ == "__main__":
    if os.name == "posix" and os.getuid() == 0:
        print("{}Do not run this as ROOT{}".format(RED, END))

    else:
        table_name = input("Enter table name: ")
        file_name = input("Enter file name [path/file.csv]: ")
        addInfo(table_name, file_name)
        