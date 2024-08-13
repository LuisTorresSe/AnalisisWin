from configparser import ConfigParser
from sqlalchemy import Engine, create_engine
import pandas as pd

def connection_sql() -> Engine:
    parser = ConfigParser()
    parser.read('pipeline.conf')
    server = parser.get('sql_credentials','server')
    database = parser.get('sql_credentials','database')
    driver = parser.get('sql_credentials','driver')
    trusted_connection =  parser.get('sql_credentials', 'trusted_connection')
    connection_string = f'mssql+pyodbc://{server}/{database}?driver={driver}&trusted_connection={trusted_connection}'
    engine = create_engine(connection_string)
    return engine

def load_data_win(conn:Engine): 
    df = pd.read_csv('../dataWin.csv', low_memory=False)
    df.to_sql('dataWin', conn, if_exists='replace',index=False)
    
def transform(conn :Engine)->list:
    df = pd.read_sql_query('SELECT [Assessed On],  [Issue Type] FROM Transform1_win', conn)
    num_rows = df.shape[0]
    dataTransform = []
    for row in range(num_rows):
        issues_list = df.iloc[row,1]
        issues = str(issues_list).split(',')
        date = df.iloc[row,0]
        for issue in issues:
            dataTransform.append({ 'Assessed On':date , 'Issue Type':issue })
              
    return dataTransform

def load_to_sql(dataTransform:list, conn:Engine)->None:
    df_to_load = pd.DataFrame(dataTransform)
    df_to_load.to_sql('Transform_issuesWin',con=conn, if_exists='replace', index=False)
    

if __name__ =='__main__':
    conn = connection_sql()
    load_data_win(conn)
    dataTransform = transform(conn)
    load_to_sql(dataTransform, conn)
    print('Operacion exitosa')
