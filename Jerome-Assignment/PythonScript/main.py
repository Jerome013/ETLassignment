from subprocess import call
import sqlite3
import pandas as pd

filename = r'C:\Users\Jerome Cura\Desktop\Jerome-Assignment' # change depend the location of file
sqliteConnection = sqlite3.connect(f'{filename}\\users.db')
cur = sqliteConnection.cursor()


def run_py_file():
    try:
        call(["python", "transactions.py"])
        call(["python", "user.py"])
        extract()
    except Exception as e:
        print('Error Extracting',{str(e)})

def extract():
    try:
        usersDf = pd.read_sql("SELECT * FROM 'usertbl'", sqliteConnection)
        trnsactnsDf = pd.read_sql("SELECT * FROM 'transactions'", sqliteConnection)
        load(usersDf,trnsactnsDf)
    except Exception as e:
        print('Error Extracting',{str(e)})

def load(df1,df2):
    try:

        sqlmainTbl = """
                    CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER,
                    name TEXT,
                    email TEXT,
                    total_spent REAL
                    )"""
        
        cur.execute(sqlmainTbl)

        mainTbl = pd.read_sql("SELECT * FROM 'users'", sqliteConnection)

        userFinal = pd.merge(df1, df2, how="inner", on=["user_id", "user_id"])
        usrFnal = userFinal.groupby(['user_id']).agg({
                "user_id" : "first",
                "name" : "first",
                "email": "first",
                "date_joined":"first",
                "amount":"sum"
                })
        usrFnal2 = usrFnal.rename(columns={'amount': 'total_spent'})
        usrFnal2.to_sql(name='users', con = sqliteConnection,if_exists='replace', index=False)

        print('Number of record/s Loaded in target Table :',len(usrFnal2) - len(mainTbl))

    except Exception as e:
        print('Error wow',{str(e)})
    finally:
        sqliteConnection.commit()
        sqliteConnection.close()

try:
    run_py_file()
except Exception as e:
    print('Error Extracting',{str(e)})