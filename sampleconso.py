#!/usr/bin/python
# -*- coding: utf-8 -*-
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
import copy
import json

print("start of the treatment ")
print("enter what you want to achieve :  ")
print("1 with index change  :  ")
print("2 with no index change  :  ")


test_text = input ("Enter a number: ")

# Converts the string into a integer. If you need
# to convert the user input into decimal format,
# the float() function is used instead of int()
test_number = int(test_text)


print("enter what you want to achieve :  ")
print("1 upsert  ?   ")
print("2 create ?  ")

choicetext = input ("Enter a number: ")
choice = int(choicetext)

url = 'https://edecosmosdb.documents.azure.com:443/'
key = '4uRxunxXFwGPLDxcxshAD0jTY5bIEJnRLsQAeC6rIVLL9q4qxmfsldnDGdHXzNGCnwoeILhdLVm8U9uzcXONUw=='
client = cosmos_client.CosmosClient(url, {'masterKey': key})
collection1 = 'dbs/edetestdatabasepython/colls/c1'
collection2 = 'dbs/edetestdatabasepython/colls/c2'
collection3 = 'dbs/edetestdatabasepython/colls/c3'
conso = 'dbs/edetestdatabasepython/colls/conso'

# uri = "mongodb://edeaxacomputetest:RD4nLlCR4PIrRcaAXuWLPhY0hM1FzWwbhrIPAaXJWlxUGGMuY0SU9TvZVN0FitixKUsTRqF3ZK4pk7U8gpmRFw==@edeaxacomputetest.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@edeaxacomputetest@"

QUERY = {
    "query": f"SELECT * from c"
}

FEEDOPTIONS = {}
FEEDOPTIONS["enableCrossPartitionQuery"] = True

# Client = 'client'

conso1 = client.QueryItems(collection1 , QUERY, FEEDOPTIONS)
if test_number == 1: 
    containerPath = 'dbs/RCS/colls/conso'
    container = client.ReadContainer(containerPath)

    container["indexingPolicy"] = {
    "indexingMode":"none",
    "includedPaths":[],
    "excludedPaths":[]
    }
    response = client.ReplaceContainer(containerPath, container)
    print('Request charge: \'{0}\' RUs'.format(client.last_response_headers['x-ms-request-charge']))

print("load the source connection customer")

for i in conso1:
    id = (i['id'])
    email = (i['email'])
    del i["_rid"]
    del i["_etag"]
    del i["_attachments"]
    del i["_self"]
    query2 = {
                "query": "SELECT * FROM r WHERE r.id=@id",
                "parameters": [ { "name":"@id", "value": str(id) } ]
             }
    conso2= client.QueryItems(collection2, query2, FEEDOPTIONS)
    print('Request charge: \'{0}\' RUs'.format(client.last_response_headers['x-ms-request-charge']))
    for j in conso2:
        c1 = copy.copy(j)
        del c1["id"]
        del c1["_rid"]
        del c1["_etag"]
        del c1["_attachments"]
        del c1["_self"]
        del c1["_ts"]
        j.clear()
        j= c1
        i.update(j)
 
    conso3 = client.QueryItems(collection3, query2, FEEDOPTIONS)
    print('Request charge: \'{0}\' RUs'.format(client.last_response_headers['x-ms-request-charge']))
    for k in conso3:
        c1 = copy.copy(k)
        del c1["id"]
        del c1["_rid"]
        del c1["_etag"]
        del c1["_attachments"]
        del c1["_self"]
        del c1["_ts"]
        k.clear()
        k = c1 
        i.update(k)
    if choice == 2:
        client.DeleteItem(conso + "/docs/" + str(id), {'partitionKey': str(id)})
        client.CreateItem(conso, i)
    else:
        client.UpsertItem(conso, i)

   # client.CreateItem("dbs/RCS/colls/pythonconso", ls)
 
    print('Request charge: \'{0}\' RUs'.format(client.last_response_headers['x-ms-request-charge']))
    print(" treatment done for the customer with id")
    print(id)
if test_number == 1: 
    container["indexingPolicy"] = {
        "indexingMode": "consistent",
        "includedPaths": [
         {
             "path": "/*"
         }
        ],
        "excludedPaths": [
         {
             "path": "/\"_etag\"/?"
         }
        ]
    }

    response = client.ReplaceContainer(containerPath, container)
    print('Request charge: \'{0}\' RUs'.format(client.last_response_headers['x-ms-request-charge']))



print("end of job ")

