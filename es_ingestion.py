from elasticsearch import Elasticsearch
from ssl import create_default_context
import os
import glob
import PyPDF2
import pandas as pd


def main():

    os.chdir(r'C:\Users\GregWilliams\OneDrive - Projecting Success Ltd\Environments\OEDT\elasticsearch_ingestion\.venv\docs_for_import')
    files = glob.glob("*.*")

    docsindf = extractpdffiles(files)

    print(docsindf)
    passtoelasticsearch(docsindf)


def extractpdffiles(files):
    
    this_loc = 1
    df = pd.DataFrame(columns = ('name','content'))

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
    
    return df


def passtoelasticsearch(df):
    
    ELASTIC_PASSWORD = "JRcinA1WZH9JJLD4S5sm"
    USER_ID = "elastic"
    CA_CERT = r'C:\Users\GregWilliams\OneDrive - Projecting Success Ltd\Environments\OEDT\elasticsearch_ingestion\ca_cert\http_ca.crt'
    
    print(os.path.isfile(CA_CERT))

    es = Elasticsearch(['https://elastic@9+VqZdlc+Y-uznPNTn_z:9200'])
    
    #attempted with security set to false in elasticsearch.yml - connection is still refused
    #es = Elasticsearch('http://localhost:9200')
    
    #es = Elasticsearch({u'host': u'127.0.0.1', u'port': u'9200', u'scheme': u'https'})

    # with security
    es = Elasticsearch(hosts=['https://localhost:9200'],
                       basic_auth=(USER_ID,ELASTIC_PASSWORD),
                       verify_certs=True,
                       ca_certs=CA_CERT,
                       request_timeout=60)

    # without security
    #es = Elasticsearch(u'https://localhost:9200')

    #es = Elasticsearch('https://localhost:9200',
    #                   ca_certs=CA_CERT,
    #                   basic_auth=(USER_ID,ELASTIC_PASSWORD))
    
    #es.info()


    nodes = [node.base_url for node in es.transport.node_pool.all()]
    print(nodes)

    col_names = df.columns
    for row_number in range(df.shape[0]):
        document = dict([(name, str(df.iloc[row_number][name])) for name in col_names])
        #print(document)
        es.index(index='hse_test1', document=document)

        ## issues = doc_types isn't being recognised
        #es.index(index='hse1',doc_type='reports', document=document)

       
if __name__ == '__main__':
    main()