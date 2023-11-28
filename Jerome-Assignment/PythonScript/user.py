import sqlite3
import pandas as pd


filename = r'C:\Users\Jerome Cura\Desktop\Jerome-Assignment' # change depend the location of file
sqliteConnection = sqlite3.connect(f'{filename}\\users.db')
cur = sqliteConnection.cursor()
#EXTRACT DATA FROM CSV
def extract():
    try:
    #Transform CSV file then update in CSV file
        trnsfrmUsr = pd.read_csv(f'{filename}\\users.csv')
        trnsfrmUsr['email'] = trnsfrmUsr['email'].str.lower()
        trnsfrmUsr.to_csv(f'{filename}\\users.csv', header=True, index=False)
        print('users.csv has: ',len(trnsfrmUsr),' Record/s')
    #Create Table in Sqlite
        sqlUsrs = """
                    CREATE TABLE IF NOT EXISTS usertbl (
                    user_id INTEGER,
                    name TEXT,
                    email TEXT,
                    date_joined NUMERIC,
                    primary key (user_id)
                    )"""
        cur.execute(sqlUsrs)

        TempSqlUsrs = """
                            CREATE TEMPORARY TABLE IF NOT EXISTS usertbl_temp
                            (user_id INTEGER,
                            name TEXT,
                            email TEXT,
                            date_joined NUMERIC)"""
        cur.execute(TempSqlUsrs)
        load()
    except Exception as e:
        print('Error Extracting',{str(e)})



def load():
    try:
        sqlUsrsInsert = ("""
                        INSERT INTO usertbl_temp VALUES(?,?,?,?)"""
                     )

        sqlUsrsInsert2 = """INSERT INTO usertbl (user_id, name, email, date_joined)
                                SELECT 
                                      user_id,name,email,date_joined
                                FROM usertbl_temp
                                WHERE user_id NOT IN (SELECT user_id FROM usertbl)"""


        sqlUsrsUpt = """UPDATE usertbl AS t1
                                SET name = t2.name,
                                    email = t2.email,
                                    date_joined = t2.date_joined
                                FROM usertbl_temp AS t2
                                WHERE t1.user_id = t2.user_id
                                    AND (t1.name <> t2.name OR t1.email <> t2.email OR
                                      t1.date_joined <> t2.date_joined)"""

        with open(f'{filename}\\users.csv','r') as file:
            file_iter = iter(file)
            header = next(file_iter)
            no_records = 0
            for row in file_iter:
                cur.execute(sqlUsrsInsert, row.split(","))
            sqliteConnection.commit()
            no_records += 0

        cur.execute(sqlUsrsUpt)
        cur.execute(sqlUsrsInsert2)
        print('Loaded successfully')
    except Exception as e:
        print('Error Loading',{str(e)})

    finally:
        sqliteConnection.commit()
        sqliteConnection.close()

try:
    extract()
except Exception as e:
        print('Error',{str(e)})