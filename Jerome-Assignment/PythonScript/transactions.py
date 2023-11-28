import sqlite3
import pandas as pd


filename = r'C:\Users\Jerome Cura\Desktop\Jerome-Assignment' # change depend the location of file
sqliteConnection = sqlite3.connect(f'{filename}\\users.db')
cur = sqliteConnection.cursor()
#EXTRACT DATA FROM CSV
def extract():
    try:
    #Transform CSV file then update in CSV file
        trnsfrmTransactn = pd.read_csv(f'{filename}\\transactions.csv')
        trnsfrmTransactn['product'] = trnsfrmTransactn['product'].str.upper()
        trnsfrmTransactn.to_csv(f'{filename}\\transactions.csv', header=True, index=False)
        print('transactions.csv has: ',len(trnsfrmTransactn),' Record/s')
    #Create Table in Sqlite
        sqlTrnsactn = """
                    CREATE TABLE IF NOT EXISTS transactions (
                    trans_id INTEGER,
                    user_id INTEGER,
                    product TEXT,
                    amount REAL,
                    trans_date NUMERIC,
                    last_update_date TIMESTAMP,
                    primary key (trans_id)
                    )"""
        cur.execute(sqlTrnsactn)

        TempTrnsactnTblDrp = """DROP TABLE IF EXISTS transactions_temp"""
        cur.execute(TempTrnsactnTblDrp)

        TempTrnsactnTbl = """
                            CREATE TEMPORARY TABLE IF NOT EXISTS transactions_temp
                            (trans_id INTEGER,
                            user_id INTEGER,
                            product TEXT,
                            amount REAL,
                            trans_date NUMERIC)"""
        cur.execute(TempTrnsactnTbl)
        load()
    except Exception as e:
        print('Error Extracting transactions',{str(e)})



def load():
    try:
        sqlTrnsactnInsert = ("""
                        INSERT INTO transactions_temp VALUES(?,?,?,?,?)"""
                     )

        sqlTrnsactnInsert2 = """INSERT INTO transactions (trans_id,user_id, product, amount, trans_date,last_update_date)
                        SELECT 
                                                  trans_id, user_id, product,amount,trans_date, 
                                                  datetime() as last_update_date
                        FROM transactions_temp WHERE trans_id NOT IN (SELECT trans_id FROM transactions)"""
        sqlTrnsactnUpt = """UPDATE transactions AS t1
                        SET user_id = t2.user_id,
                            product = t2.product,
                            amount = t2.amount,
                            trans_date = t2.trans_date,
                            last_update_date = datetime()
                        FROM transactions_temp AS t2
                        WHERE t1.trans_id = t2.trans_id
                            AND (t1.user_id <> t2.user_id OR t1.product <> t2.product OR
                              t1.amount <> t2.amount OR t1.trans_date <> t2.trans_date)"""
        
        with open(f'{filename}\\transactions.csv','r') as file:
            file_iter = iter(file)
            header = next(file_iter)
            no_records = 0
            for row in file_iter:
                cur.execute(sqlTrnsactnInsert, row.split(","))
            sqliteConnection.commit()
            no_records +=0

            cur.execute(sqlTrnsactnUpt)
            cur.execute(sqlTrnsactnInsert2)
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