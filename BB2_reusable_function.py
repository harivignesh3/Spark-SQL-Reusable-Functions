# Databricks notebook source
from pyspark.sql import *
from pyspark.sql.session import SparkSession

def get_sparksession(app): #resuable function
    spark = SparkSession.builder.master("local[*]").appName("app").enableHiveSupport().config("spark.jars","/usr/local/spark/jars/mysql-connector-java.jar").getOrCreate()
    return spark

def read_data(type,sparksession,source,datatype,infersch=False,head=False, delim=',',mod='failfast'): #reusable function
    if type=='csv' and datatype != '':
        df=sparksession.read.csv(source, schema=datatype, sep=delim, inferSchema=infersch, header=head, mode=mod)
        return df
    elif type=='csv':
        df=sparksession.read.csv(source, sep=delim, inferSchema=infersch, header=head, mode=mod)
        return df
    elif type=='json':
        df=sparksession.read.option("multiline","True").json(source)
        return df
'''    
df =read_data("csv", spark_session, "dbfs:/FileStore/Data/custsmodified", infersch=True, head=False, datatype=None, delim=',', mod='permissive')
df.show()

from pyspark.sql.types import StructType,StructField,StringType,IntegerType
cussch=StructType([StructField("id",IntegerType(),True),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType(),True)])
df1=read_data("csv", spark_session, "dbfs:/FileStore/Data/custsmodified", head=False, datatype=cussch, delim=',', mod='permissive')
df1.show()
'''

def dedup(df,cols,sub,asc=True): #reusable function
    dup=df.sort(cols,ascending=asc).dropDuplicates(subset=sub)
    return dup

'''
df_dedup=dedup(df1,'age',sub=['id'])
df_dedup.show()
'''


def optimize_performance(df,spk,numpart,partflag,cacheflag,numshufflepart=200):
    print("Number of partitions in the dataframe".format(df.rdd.getNumPartitions()))
    if partflag:
        optimize_df=df.repartition(numpart)
        print(f"repartitioned to {df.rdd.getNumPartitions()}")
    else:
        optimize_df=df.coalesce(numpart)
        print(f"coalesced to {df.rdd.getNumPartitions()}")
    if cacheflag:
        df.cache()
        print("cached")
    if numshufflepart!=200:  # default partions to 200 after shuffle happens because of some wide transformation spark sql uses in the background
        spk.conf.set("spark.sql.shuffle.partitions", numshufflepart)
        print(f"shuffle partition to {numshufflepart}")
        return optimize_df



def munge(df,dictionary,drop,fill,replace,coltype='all'): #reusable function
    df_drop=df.na.drop(coltype,subset=drop)
    df_fill=df_drop.na.fill("na",subset=fill)
    df_replace=df_fill.na.replace(dictionary,subset=replace)
    return df_replace


def age_conversion(age): #Python Function for UDF conversion
    if age<=13:
        return "Children"
    elif age>13 and age<=18:
        return "Teen"
    else:
        return "Adult"


def fil(df,condition): # reusable function
    return df.filter(condition)


def mask_fields(df,cols,masktype,bits=-1):  #df.withColumn(i[0],masktype(i[0])).withColumn(i[1],masktype(i[1]))
    for i in cols:
        if (bits<0):
         df=df.withColumn(i,masktype(i))
        else:
         df=df.withColumn(i,masktype(i,bits))
    return df


from configparser import *  #from pyspark.sql.session import *
#writeRDBMSData(df_json,"/home/hduser/connection.prop","custdb","ebay_jsondata","overwrite","DEVDBCRED")
#CONFIG DRIVEN APPROACH
def writeRDBMSData(df,propfile,db,tbl,mode,env):
    # Creating a object (config) by instantiating/constructing a class called ConfigParser kept inside the module calledconfigparser
    config = ConfigParser()#instantiate the ConfigParser() into the memory area in the name of config
    config.read(propfile)#read the property file and load inside the config object (memory area)
    driver=config.get(env, 'driver')#'com.mysql.cj.jdbc.Driver'
    host=config.get(env, 'host')#jdbc:mysql://127.0.0.1
    port=config.get(env, 'port')
    user=config.get(env, 'user')
    passwd=config.get(env, 'pass')
    url=host+":"+port+"/"+db
    url1 = url+"?user="+user+"&password="+passwd
    df.write.jdbc(url=url1, table=tbl, mode=mode, properties={"driver": driver})
    #df.write.jdbc(url="jdbc:mysql://127.0.0.1:3306/custdb?user=root&password=Root123$", table="ebay_jsondata", mode="overwrite",
    #                   properties={"driver": "com.mysql.jdbc.Driver"})
