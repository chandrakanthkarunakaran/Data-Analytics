import boto3
import pandas as pd

def TabletoDynamoDBJson(data):

    "converts dataframe to dynamodb compatible json"

    pk="TransactionID"

    sk="Item"

    if any([x not in list(data.columns) for x in [pk,sk]]):

        raise Exception ("Partition Key Or Sort Key Missing In Data")
    
    dynamoDBJson=[]

    for iD,row in data.iterrows():

        objRow=dict(row)

        objIteration={}

        for key,val in objRow.items():

            if type(val) in [int,float]:

                objIteration[key]={"N":str(val)}
            
            elif pd.isnull(val)==True:

                objIteration[key]={"NULL":True}
            
            else:

                objIteration[key]={"S":str(val)}
        
        dynamoDBJson.append(objIteration)
    

    # batchwise split

    batchRecords=[dynamoDBJson[n:n+20] for n in range(0, len(dynamoDBJson), 20)]
    

    return batchRecords


def ConvertTransactionToDynamoJson(transaction):

    "converts transaction to a dynamodb compatible json"

    dynamoDB=boto3.client("dynamodb",region_name="us-east-2")

    transactionTable=pd.DataFrame.from_dict(transaction["Items"])

    for key,val in transaction.items():

        if key=="Items":

            continue

        transactionTable.loc[:,key]=val

    # Columns    

    transactionTable.columns=[x.lower() for x in list(transactionTable.columns)]

    mapPKSK={"transactionid":"TransactionID","item":"Item"}

    transactionTable.rename(columns=mapPKSK,inplace=True)

    # Batch DynamoDB Json

    batchRecords=TabletoDynamoDBJson(transactionTable)

    # Push To DynamoDB

    for batch in batchRecords:

        respBatchWrite=dynamoDB.batch_write_item(RequestItems={
            "Transactions":[{"PutRequest":{"Item":x}} for x in batch]})
    
    

    return {"Status":"Transaction Posted To Database",
            "TransactionID":transaction['transactionid']}


def LambdaHandler(event,context):

    "receives transaction and posts it to dynamodb"

    return ConvertTransactionToDynamoJson(event)



