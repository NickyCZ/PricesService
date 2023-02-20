import boto3
import pandas as pd
from datetime import datetime
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext

# create a DynamoDB client
logger = Logger()
app = APIGatewayRestResolver()
dynamodb = boto3.resource("dynamodb")

@app.post("/daily_prices")
def daily_prices_processing():
    data = app.current_event.json_body
    instrument = data['instrument']
    start_time = int(data["start_time"])
    multiple_prices = retrieve_multiple_prices_from_dynamodb(instrument, start_time)
    daily_prices = aggregate_to_day_based_prices(multiple_prices)
    write_daily_prices(daily_prices)
    
def retrieve_multiple_prices_from_dynamodb(instrument: str, start_time: int) -> dict: 
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

def aggregate_to_day_based_prices(multiple_prices: dict) -> pd.Series:
    df = pd.DataFrame.from_dict(multiple_prices)
    series = df.set_index('UnixDateTime')
    series.index = pd.to_datetime(pd.to_numeric(series.index), unit='s')
    series['Price'] = pd.to_numeric(series['Price'])
    daily_summary = series.resample('D').mean()
    return daily_summary
    
def write_daily_prices(dailyPrices: dict):
    table_name = "daily_prices"
    # initiate batch_items
    batch_items = []
    for item in dailyPrices:
        batch_items.append({'PutRequest': {'Item': item}})
        copied_items += 1
        # write to destination
        dynamodb.batch_write_item(RequestItems={table_name: batch_items})
        # reset the batch_items after writting
        batch_items = []

    # final write if any items remaining
    if len(batch_items) > 0:
        dynamodb.batch_write_item(RequestItems={table_name: batch_items})
        copied_items += len(batch_items)

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)