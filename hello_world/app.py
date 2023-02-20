import boto3
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from datetime import datetime

# create a DynamoDB client
logger = Logger()
app = APIGatewayRestResolver()
dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    
    instrument = event['instrument']
    dst_table_name = 'multiple_prices'
    src_table_name = instrument
    
    copied_items = 0
    # retrieve all items from the source table
    table = dynamodb.Table(src_table_name)
    
    items = []
    response = table.query(
        KeyConditionExpression=Key('Instrument').eq(instrument)
    )
    items += response['Items']
    while 'LastEvaluatedKey' in response:
        response = table.query(
            KeyConditionExpression=Key('Instrument').eq(instrument),
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        items += response['Items']
    
    # initiate batch_items
    batch_items = []
    for item in items:
        batch_items.append({'PutRequest': {'Item': item}})
        copied_items += 1
        # write to destination
        dynamodb.batch_write_item(RequestItems={dst_table_name: batch_items})
        # reset the batch_items after writting
        batch_items = []

    # final write if any items remaining
    if len(batch_items) > 0:
        dynamodb.batch_write_item(RequestItems={dst_table_name: batch_items})
        copied_items += len(batch_items)
    print(f"{copied_items} items are copied from {src_table_name} to {dst_table_name}")
    
    if copied_items != len(items):
        print(f"Error: Only {copied_items} out of {len(items)} items were copied.")
    else:
        print(f"Success: {copied_items} items are copied from {src_table_name} to {dst_table_name}")

def retrieve_prices_from_dynamodb(instrument: str, start_time: int) -> dict: 
    table_name = "multiple_prices"
    end_time = int(datetime.now().timestamp())
    try:
        table = dynamodb.Table(table_name)
        items = []
        response = table.query(
            KeyConditionExpression=Key('Instrument').eq(instrument) & Key('UnixDateTime').between(start_time, end_time),
            ProjectionExpression='UnixDateTime,Price'
        )
        items += response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('Instrument').eq(instrument) & Key('UnixDateTime').between(start_time, end_time),
                ProjectionExpression='UnixDateTime,Price',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items += response['Items']
        return items

    except Exception as e:
        # log the error message
        logger.error("Error occurred while retrieving prices from DynamoDB", e)
        raise ValueError("Error occurred while retrieving prices from DynamoDB")