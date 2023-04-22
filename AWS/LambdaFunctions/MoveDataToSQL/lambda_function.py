import pandas as pd
import boto3
import json

dynamoDB=boto3.client("dynamodb")
s3=boto3.client("s3")

def DynamoDBTable(transactionid):

    "retrieves transactionid from dynamoDB"

    query="select * from Transactions where TransactionID='%s'"%transactionid

    results=dynamoDB.execute_statement(Statement=query)

    return results

def Djson2HiveJson(response):

    "converts dynamodb json to hive json"

    hiveJsonL=[]

    for item in response["Items"]:

        obj={}

        for key,val in item.items():

            if "S" in val.keys():

                obj[key]=val["S"]
            
            else:

                obj[key]=float(val["N"])
        
        hiveJsonL.append(obj)
    

    jsonH=json.dumps(hiveJsonL)

    jsonH=jsonH.replace("}, ","}\n")

    jsonH=jsonH[1:-1]

    

    return jsonH


def LambdaHandler(event,context):

    '''runs partiQL query on dynamodb for a transactionID and 
    posts the records as Athena Compatible json'''

    transactionID=event["TransactionID"]

    transactionTable=DynamoDBTable(transactionID)

    jsonTransaction=Djson2HiveJson(transactionTable)

    s3.put_object(Body=jsonTransaction,Bucket="data-analytics-ck",
              Key="TransactionTable/%s.json"%transactionID)
    

    return {"Status":"Success"}




