import os
import glob
import PyPDF2
import pandas as pd
import psycopg2
from psycopg2.extras import Json
from pyscopg2.extensions import register_adapter
import json

def main():

    #os.chdir(r'.venv\docs_for_import')
    #files = glob.glob("*.pdf")

    #docsindf = extractpdffiles(files)
    #convert_df_to_json(docsindf)
    conn = connecttopg()
    import_json_into_pg(conn)
 



def extractpdffiles(files):
    
    this_loc = 1
    #df = pd.DataFrame(columns = ('doc_name','doc_content'))
    df = pd.DataFrame(columns = ('doc_name','doc_content'))
    for file in files:
        pdfFileObj = open(file,'rb')
        pdfReader = PyPDF2.PdfReader(pdfFileObj)
        n_pages = len(pdfReader.pages)
        this_doc = ''

        for i in range(n_pages):
            pageObj = pdfReader.pages[i]
            this_text = pageObj.extract_text()
            this_doc += this_text
        df.loc[this_loc] = file, this_doc
        this_loc = this_loc + 1
    df.reset_index(drop=True, inplace=True)
    return df


def connecttopg():
    conn = psycopg2.connect(
        host="oedt-storage-dev.postgres.database.azure.com",
        database="oedt",
        user="postgresadmin",
        password="Sardonic1789!")

    return conn 

    cur = connection.cursor()

    print('Postgres database version:')
    cur.execute('SELECT version()')

    db_version = cur.fetchone()
    print(db_version)

    cur.close()

def convert_df_to_json(df):
    with open(r'C:\Users\GregWilliams\OneDrive - Projecting Success Ltd\Environments\OEDT\elasticsearch_ingestion\.venv\json_output\json_sample.json', 'w') as f:
        f.write(df.to_json(orient='records',lines=True))

def import_json_into_pg(conn):
    with open(r'C:\Users\GregWilliams\OneDrive - Projecting Success Ltd\Environments\OEDT\elasticsearch_ingestion\.venv\json_output\json_sample.json') as f:
        data = json.load(f)
    print(type(data))
    cur = conn.cursor() 

    query_sql = """INSERT INTO hse_docs (json_data) VALUES (%s)""", ({"abc":"test_again"})

    cur.execute(query_sql)

    conn.commit()
    cur.execute('select * from hse_docs')
    print(cur.fetchall())

    cur.close()
    conn.close()
    #     with conn.cursor as cur:
    #    data = [json.loads(line) for line in open(r'C:\Users\GregWilliams\OneDrive - Projecting Success Ltd\Environments\OEDT\elasticsearch_ingestion\.venv\json_output\json_sample.json')]
    #    for line in data:
    #        print(line)
    #        cur.execute('''INSERT INTO hse_docs (line) VALUES ({%s})''', [json.dumps(line)] )
    #
    #        conn.commit()
    #cur.close()
    #conn.close() """


if __name__ == '__main__':
    main()