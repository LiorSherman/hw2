from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
es = Elasticsearch(HOST="http://localhost",PORT=9200)
import csv  
import json 

ES_HOST = {"host" : "localhost", "port" : 9200}
INDEX_NAME1='redding'
INDEX_NAME2='buttonwillow'
INDEX_NAME3='sandiego'
INDEX_NAME4='modesto'
TYPE_NAME = ' state'
ID_FIELD = 'id'
consumer = KafkaConsumer('FPA',bootstrap_servers='104.209.178.73:5601',auto_offset_reset = 'earliest')


header=[]
bulk_data = []
bulk_data2 = []
bulk_data3 = [] 
bulk_data4 = []
dict_json={}
for msg in consumer:
    data_dict = {}
    msgStr=msg.value.decode('ASCII')
    msgStr=msgStr[1:-1]
    msgStr=msgStr.replace("'","")
    msgLst=msgStr.split(",")
    print (msgLst)
    data_dict = {}
    if (msgLst[1]==TYPE_NAME):
        for j in range(len(msgLst)):
            header.append(msgLst[j])
            header = [item.lower() for item in header]
    
    
    ### condition1
    if((msgLst[16]==' TRUE') and (msgLst[2]>' 2016-01-30')):
        if (msgLst[0]!='id'):
            for i in range(len(msgLst)):
                data_dict[header[i]] = msgLst[i]
            op_dict = {
                "index": {
                    "_index":INDEX_NAME1, 
                    "_type": 'state', 
                    "_id": data_dict[ID_FIELD]
                    }
            }
            bulk_data.append(op_dict)
            bulk_data.append(data_dict)

        from elasticsearch import Elasticsearch
        # create ES client, create index
        es = Elasticsearch(hosts = [ES_HOST])
        if es.indices.exists(INDEX_NAME1):
            print("deleting '%s' index..." % (INDEX_NAME1))
            res = es.indices.delete(index = INDEX_NAME1)
            print(" response: '%s'" % (res))
        # since we are running locally, use one shard and no replicas
        request_body = {
            "settings" : {
                "number_of_shards": 1,
                "number_of_replicas": 0
            }
        }


        print("creating '%s' index..." % (INDEX_NAME1))
        res = es.indices.create(index = INDEX_NAME1, ignore=400,body = request_body)
        print(" response: '%s'" % (res))
        # bulk index the data
        print("bulk indexing...")
        res = es.bulk(index = INDEX_NAME1, body = bulk_data, refresh = True)
        # sanity check
        res = es.search(index = INDEX_NAME1, size=2, body={"query": {"match_all": {}}})
        print(" response: '%s'" % (res))

        
    else:   
        #### condition2   
        data_dict2={}
        if (msgLst[0]!='id'):
            data_dict2[header[0]] = msgLst[0]
            data_dict2[header[4]] = msgLst[4]
            op_dict2 = {
                "index": {
                    "_index":INDEX_NAME2, 
                    "_type": 'state', 
                    "_id": data_dict2[ID_FIELD]
                    }
                }
            bulk_data2.append(op_dict2)
            bulk_data2.append(data_dict2)

            from elasticsearch import Elasticsearch
            # create ES client, create index
            es = Elasticsearch(hosts = [ES_HOST])
            if es.indices.exists(INDEX_NAME2):
                print("deleting '%s' index..." % (INDEX_NAME2))
                res = es.indices.delete(index = INDEX_NAME2)
                print(" response: '%s'" % (res))
            # since we are running locally, use one shard and no replicas
            request_body = {
                "settings" : {
                    "number_of_shards": 1,
                    "number_of_replicas": 0
                }
            }
            
            print("creating '%s' index..." % (INDEX_NAME2))
            res = es.indices.create(index = INDEX_NAME2, ignore=400,body = request_body)
            print(" response: '%s'" % (res))
            # bulk index the data
            print("bulk indexing...")
            res = es.bulk(index = INDEX_NAME2, body = bulk_data2, refresh = True)
            # sanity check
            res = es.search(index = INDEX_NAME2, size=2, body={"query": {"match_all": {}}})
            print(" response: '%s'" % (res))
            
        #### condition3  
        if(msgLst[9]==' F'):
            data_dict3={}
            if (msgLst[0]!='id'):
                for i in range(len(msgLst)):
                    data_dict3[header[i]] = msgLst[i]
                op_dict3 = {
                    "index": {
                        "_index":INDEX_NAME3, 
                        "_type": 'state', 
                        "_id": data_dict3[ID_FIELD]
                        }
                    }
                bulk_data3.append(op_dict3)
                bulk_data3.append(data_dict3)

                from elasticsearch import Elasticsearch
                # create ES client, create index
                es = Elasticsearch(hosts = [ES_HOST])
                if es.indices.exists(INDEX_NAME3):
                    print("deleting '%s' index..." % (INDEX_NAME3))
                    res = es.indices.delete(index = INDEX_NAME3)
                    print(" response: '%s'" % (res))
                # since we are running locally, use one shard and no replicas
                request_body = {
                    "settings" : {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
                
                print("creating '%s' index..." % (INDEX_NAME3))
                res = es.indices.create(index = INDEX_NAME3, ignore=400,body = request_body)
                print(" response: '%s'" % (res))
                # bulk index the data
                print("bulk indexing...")
                res = es.bulk(index = INDEX_NAME3, body = bulk_data3, refresh = True)
                # sanity check
                res = es.search(index = INDEX_NAME3, size=2, body={"query": {"match_all": {}}})
                print(" response: '%s'" % (res))
        
        #### condition4  
        if(msgLst[9]==' M'):
            
            data_dict4={}
            if (msgLst[0]!='id'):
                for i in range(len(msgLst)):
                    data_dict4[header[i]] = msgLst[i]
                op_dict4 = {
                    "index": {
                        "_index":INDEX_NAME4, 
                        "_type": 'state', 
                        "_id": data_dict4[ID_FIELD]
                        }
                    }
                bulk_data4.append(op_dict4)
                bulk_data4.append(data_dict4)

                from elasticsearch import Elasticsearch
                # create ES client, create index
                es = Elasticsearch(hosts = [ES_HOST])
                if es.indices.exists(INDEX_NAME4):
                    print("deleting '%s' index..." % (INDEX_NAME4))
                    res = es.indices.delete(index = INDEX_NAME4)
                    print(" response: '%s'" % (res))
                # since we are running locally, use one shard and no replicas
                request_body = {
                    "settings" : {
                        "number_of_shards": 1,
                        "number_of_replicas": 0
                    }
                }
                print("creating '%s' index..." % (INDEX_NAME4))
                res = es.indices.create(index = INDEX_NAME4, ignore=400,body = request_body)
                print(" response: '%s'" % (res))
                # bulk index the data
                print("bulk indexing...")
                res = es.bulk(index = INDEX_NAME4, body = bulk_data4, refresh = True)
                # sanity check
                res = es.search(index = INDEX_NAME4, size=2, body={"query": {"match_all": {}}})
                print(" response: '%s'" % (res))

