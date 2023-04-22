import pandas as pd
import boto3

athena=boto3.client("athena")


def DailyReportGeneration():

    "runs athena query and generates daily sales report"

    currentDate=str(pd.Timestamp.now())[:10]

    # Report Generation Query

    query='''select date as "Transaction Date",
                item as "Item",
                count (distinct transactionid) as "Num Of Transactions",
                count(distinct customerid) as "Num Of Customer ID",
                sum(qty) as "Sold Units",
                sum(amount) as "Revenue"
            from transactions
            where date=date('%s')
            group by date,
                item,
                transactionid,
                customerid'''%currentDate

    # Run Query On Athena

    respSQL=athena.start_query_execution(QueryString=query,
            ClientRequestToken=str(pd.Timestamp.now().value).ljust(35,"0"),
            QueryExecutionContext={
                'Database': 'transactionsystem',
                'Catalog': 'awsdatacatalog'
            },
            ResultConfiguration={
                'OutputLocation': 's3://data-analytics-ck/DailySalesReport/'
            })
    
    # Output Location

    opLocation= 's3://data-analytics-ck/DailySalesReport/'+respSQL["QueryExecutionId"]+'.csv'
    

    return {"Status":"Report Generated Successfully",
            "Report Date":currentDate,
            "File Location":opLocation}

    
    
def LambdaHandler(event,context):

    "runs daily report"

    return DailyReportGeneration()