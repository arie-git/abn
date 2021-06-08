#!/usr/bin/env python
# coding: utf-8

# In[25]:


#libary & logging
from pyspark.sql import SparkSession
from functools import reduce
import logging
import sys
from logging.handlers import TimedRotatingFileHandler
import configparser

#config file that contains parameters such as dataset and countries to filter on
#config = configparser.ConfigParser()
#config.read('config.ini')

#logging formatting and filename


#arguments to rename and drop columns 
newColumns =['client_identifier','email', 'country', 'bitcoin_address', 'credit_card_type']
columns_to_drop=['first_name','last_name','cc_n']
list_countries=['United Kingdom','Netherlands']
#logging args

def main():
    logger = get_logger("ABN exercise")
    spark = create_session()
    
    df1 = spark.read.csv("inputdata/dataset_one.csv",header=True)
    df2 = spark.read.csv("inputdata/dataset_two.csv",header=True)
    
    logger.info("loaded datasets")
    
    data = df1.join(df2,'id')
    logger.info("joined datasets")

    
    droppeddata = data.drop(*columns_to_drop)
    logger.info("dropped columns")
    
    
    renameddata=renameColumns(droppeddata,newColumns)
    #renameddata.show(5)
    logger.info("renamed columns")
    
    #get list from config ini
    #filterlist=config['filters']['countries'].split(',')
    
    filtereddata=filterCountry(renameddata, list_countries)
    #config['filters']['countries'].split(';')
    logger.info("filtered data by specified countries")
    
    filtereddata.write.csv('client_data',header='true', mode='overwrite')
    
    logger.info("writing to client_data folder")    
    
    #return None
    
    
#filter dataset to countries supplied in arguments
def filterCountry(df,loc):
    #'''Takes dataframe and array of desired countries as input, returns filterd dataframe'''
    return df.filter(df.country.isin(loc))
    #return result


#rename columns
def renameColumns(df,newcol):
    #'''Takes dataframe and list of new columns as input, returns dataframe with renamed columns. Number of input / rename columns must be equal!'''
    oldColumns = df.schema.names
    renamedresult = reduce(lambda df, idx: df.withColumnRenamed(oldColumns[idx], newcol[idx]), range(len(oldColumns)), df)
    return renamedresult

def create_session():
    spk = SparkSession.builder         .master("local")         .appName("Filter_values.com")         .getOrCreate()
    return spk

#logging logic: 
#https://www.toptal.com/python/in-depth-python-logging
FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")

def get_console_handler():
   #'''Setup console handler and formatting'''
   console_handler = logging.StreamHandler(sys.stdout)
   console_handler.setFormatter(FORMATTER)
   return console_handler
def get_file_handler():
    #'''Setup log file handler and formatting'''
   file_handler = TimedRotatingFileHandler('abn.log', when='midnight')
   file_handler.setFormatter(FORMATTER)
   #file_handler.suffix="%Y%m%d%" 
   return file_handler
def get_logger(logger_name):
    #'''set log level, currently on DEBUG'''
   logger = logging.getLogger(logger_name)
   logger.setLevel(logging.DEBUG) # debug level logging
   logger.addHandler(get_console_handler())
   logger.addHandler(get_file_handler())
   # with this pattern, it's rarely necessary to propagate the error up to parent
   logger.propagate = False
   return logger


if __name__ == '__main__':
    main()
    
    


# In[ ]:





# In[ ]:




