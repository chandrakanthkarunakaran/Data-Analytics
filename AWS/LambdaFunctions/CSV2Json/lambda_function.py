# Packages Required

import pandas as pd
import boto3
import io

# AWS Services

s3=boto3.client("s3")
dynamoDB=boto3.client("dynamodb")
sns=boto3.client("sns")

# Main Function

def CSVtoDynamoDBJson(data):

    "converts dataframe to dynamodb compatible json"

    pk="customerid"

    sk="transactionid"

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



def LambdaHandler(event,context):

    "lambda handler that converts csv to json"

    print("Received Event:",event)

    bucketName=event["Records"][0]["s3"]["bucket"]["name"]

    key=event["Records"][0]["s3"]["object"]["key"]

    # Reading File From S3

    file=s3.get_object(Bucket=bucketName,Key=key)
    
    ioObj=io.BytesIO(file['Body'].read())

    # Converting To A DataFrame

    data=pd.read_csv(ioObj, encoding='utf-8')

    batchRecords=CSVtoDynamoDBJson(data)

    for batch in batchRecords:

        respBatchWrite=dynamoDB.batch_write_item(RequestItems={
            "Transactions":[{"PutRequest":{"Item":x}} for x in batch]})
    

    print("Transaction Records Posted")

    return {"Status":"Success"}




    
    # except Exception as E:

    #     event["Status"]="Failed"

    #     event["Reason"]=str(E)

    #     # Publish Failed Transaction To SNS

    #     respPublish=sns.publish(TopicArn="arn:aws:sns:us-east-2:073914561898:FailedTransactions",
    #                             Message="Failed To Post Transaction Records From The File",
    #                             Subject="Failed Transaction",
    #                             MessageAttributes={key:{
    #                                                     'DataType': 'string',
    #                                                     'StringValue': val
    #                                                 } for key,val in event.items()},
    #                             MessageDeduplicationId=str(pd.Timestamp.now.value()),
    #                             MessageGroupId="FAILEDTRANS202303")
        
    #     print("Message Posted On SNS")




