---
page_type: sample
languages:
- python
products:
- azure-cosmosdb 
description: "This sample demonstrates a Python application that will be make a 360 view for the enduser "

---
# Developp a 360 view application in python with azure cosmosdb 

## About this sample

> This sample is based on a real client use case i have made few week ago

### Overview

This sample demonstrates a Python application that will generate a consolidated view 

1. a python generator using faker library 
2. an application that will consolidate 3 collection in one.



## How to run this sample

To run this sample, you'll need:

> - [Python 2.7+](https://www.python.org/downloads/release/python-2713/) or [Python 3+](https://www.python.org/downloads/release/python-364/)

> - An Azure cosmosdb account 


### Step 1:  Clone or download this repository

From your shell or command line:

```Shell
git clone https://github.com/emdeletaede/build360-view-with-cosmosdb.git
```

or download and extract the repository .zip file.

> Given that the name of the sample is quite long, you might want to clone it in a folder close to the root of your hard drive, to avoid file name length limitations when running on Windows.

### Step 2:  install the pre-requisite python library 


- You will need to install dependencies using pip as follows:
```Shell
$ python -m pip install faker
$ Python -m pip install azure-cosmos

```

change the db name , the key and endpoint of your cosmosdb . 
### Step 3:  Run the application  

Run the generator to generate data in c1, C2,C3  collections and the conso collection will be create , after launch the sampleconso.py that make the consolidation 

```Shell
$ python generator.py

$ python sampleconso.py 

```


in addition a word with more information will arrive in the future 


