from cfgparser import cfgparser
import pyodbc
import pandas as pd
from datetime import date

class db():
    # some constants for building connection string
    driver_dict = {
        'SQL Server': '{SQL Server}',
    }

    def __init__(self):
        self.db = None
        self.cnxn = None
        self.cursor = None
        self.credentials = None

    def get_credentials(self, *args, **kwargs):
        # read test.ini to get credentials
        return cfgparser.read_config(*args, **kwargs)

    def connect_db(self, server_type):
        # connect to db
        cnxn = None

        # select server type
        driver = driver_dict[server_type]

        # connection strings
        cnxn_parameters = []
        cnxn_parameters.append(f"DRIVER={driver}")
        cnxn_parameters.append(f"SERVER={self.credentials['server']}")
        cnxn_parameters.append(f"DATABASE={self.credentials['db']}")
        cnxn_parameters.append(f"UID={self.credentials['user']}")
        cnxn_parameters.append(f"PWD={self.credentials['pass']}")
        cnxn_string = ';'.join(cnxn_parameters)

        # make connection
        cnxn = pyodbc.connect(cnxn_string)
        self.clear_pass()
        return cnxn

    def clear_pass(self):
        # remove password
        self.credentials['pass'] = ''

    def set_cursor(self):
        cursor = self.cnxn.cursor()
        return cursor

    def test_connection(self):
        # test connection
        test_query = 'select 1'
        cursor = self.set_cursor()
        try:
            cursor.execute(test_query)
            cursor.close()
            print('Connected')
            return 'Connected'
        except:
            print('Not connected')
            return 'Failed'

    def query(self, query_string):
        cnxn = self.cnxn
        df = pd.read_sql(query_string, cnxn)
        return df

if __name__ == '__main__':
    db = db()

    # connect to db and test connection
    MAX_TRIES = 3
    TRIES = 0
    CONNECTED = False
    while not CONNECTED:
        db.credentials = db.get_credentials(filename='cfg.ini', section='db')
        db.cnxn = db.connect_db('SQL Server')
        CONNECTED = db.test_connection()

        TRIES += 1
        if TRIES >= MAX_TRIES: break

    # exit program if not connected
    if not CONNECTED:
        print(f"unable to connect after {TRIES}")
        exit()

    # do the query
    table = 'table
    query_string = f"select * from {table}"
    df = db.query(query_string)

    # save results to file
    base_path = ''
    save_path = f"{base_path}/result_{date.today().strftime('%Y_%m_%d')}.csv"
    df.to_csv(save_path, index=False)