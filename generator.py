#!/usr/bin/python
# -*- coding: utf-8 -*-
import azure.cosmos.cosmos_client as cosmos_client
import azure.cosmos.documents as documents
import azure.cosmos.errors as errors
import azure.cosmos.http_constants as http_constants
from faker import Faker
import uuid
import copy
import json
from datetime import datetime






url = 'https://edecosmosdb.documents.azure.com:443/'
key = '4uRxunxXFwGPLDxcxshAD0jTY5bIEJnRLsQAeC6rIVLL9q4qxmfsldnDGdHXzNGCnwoeILhdLVm8U9uzcXONUw=='
client = cosmos_client.CosmosClient(url, {'masterKey': key})

# uri = "mongodb://edeaxacomputetest:RD4nLlCR4PIrRcaAXuWLPhY0hM1FzWwbhrIPAaXJWlxUGGMuY0SU9TvZVN0FitixKUsTRqF3ZK4pk7U8gpmRFw==@edeaxacomputetest.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@edeaxacomputetest@"

QUERY = {
    "query": f"SELECT  value count(1) FROM c"
}

FEEDOPTIONS = {}
FEEDOPTIONS["enableCrossPartitionQuery"] = True



database_name = 'edetestdatabasepython'
try:
    database = client.CreateDatabase({'id': database_name})
except errors.HTTPFailure:
    database = client.ReadDatabase("dbs/" + database_name)


container_c1 = {'id': 'c1',
                        'partitionKey':
                                    {
                                        'paths': ['/id'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                        }


container_c2 = {'id': 'c2',
                        'partitionKey':
                                    {
                                        'paths': ['/id'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                        }

container_c3 = {'id': 'c3',
                        'partitionKey':
                                    {
                                        'paths': ['/id'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                        }


container_conso = {'id': 'conso',
                        'partitionKey':
                                    {
                                        'paths': ['/id'],
                                        'kind': documents.PartitionKind.Hash
                                    }
                        }


try:
    container1 = client.CreateContainer("dbs/edetestdatabasepython", container_c1, {'offerThroughput': 1000})
    container2 = client.CreateContainer("dbs/edetestdatabasepython", container_c2, {'offerThroughput': 1000})
    container3 = client.CreateContainer("dbs/edetestdatabasepython", container_c3, {'offerThroughput': 1000})
    containerconso = client.CreateContainer("dbs/edetestdatabasepython", container_conso, {'offerThroughput': 1000})


except errors.HTTPFailure as e:
    if e.status_code == http_constants.StatusCodes.CONFLICT:
        container = client.ReadContainer("dbs/edetestdatabasepython" + "/colls/c1" )
    else:
        raise e


fake = Faker()
Faker.seed(0)
di = dict()
cout = 0.0

now = datetime.now()

for i in range(1, 100):
    di2 = fake.pydict(variable_nb_elements=True, value_types=['str',int,bool])
    di["id"] = str(i)
    di['productName'] = fake.name()
    di["email"] = fake.email()
    di["bank country"] = fake.bank_country()
    di["country"] = fake.country()
    di["s1"] = di2
    client.UpsertItem("dbs/edetestdatabasepython/colls/c1",  di)
    cout = cout +  float (client.last_response_headers['x-ms-request-charge'])
    di.clear()


now2 = datetime.now()
print ("Time for c1 ")
print (now2 - now)
print ("cout pour c1 ")
print (cout)


cout = 0.0

now = datetime.now()

for i in range(1, 100):
    di2 = fake.pydict(variable_nb_elements=True, value_types=['str',int,bool])
    di["id"] = str(i)
    di['productName'] = fake.name()
    di["email"] = fake.email()
    di["plate"] = fake.license_plate()
    di["s2"] = di2
    client.UpsertItem("dbs/edetestdatabasepython/colls/c2",  di)
    cout = cout +  float (client.last_response_headers['x-ms-request-charge'])
    di.clear()

now2 = datetime.now()
print ("Time for c2 ")
print (now2 - now)
print ("cout pour c2 ")
print (cout)


cout = 0.0

for i in range(1, 100):
    di2 = fake.pydict(variable_nb_elements=True, value_types=['str',int,bool])
    di["id"] = str(i)
    di['productName'] = fake.name()
    di["email"] = fake.email()
    di["s3"] = di2
    client.UpsertItem("dbs/edetestdatabasepython/colls/c3",  di)
    cout = cout +  float (client.last_response_headers['x-ms-request-charge'])
    di.clear()

now2 = datetime.now()
print ("Time for c3 ")
print (now2 - now)
print ("cout pour c3 ")
print (cout)
