import boto3
import pandas as pd
from datetime import datetime
from decimal import Decimal
from boto3.dynamodb.conditions import Key
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler import APIGatewayRestResolver
from aws_lambda_powertools.logging import correlation_paths
from aws_lambda_powertools.utilities.typing import LambdaContext

# create a DynamoDB client
logger = Logger()
app = APIGatewayRestResolver()
dynamodb = boto3.resource("dynamodb")

source_table = "multiple_prices"
target_table = "daily_prices"

@app.post("/daily_prices_processing")
def daily_prices_processing():
    data = app.current_event.json_body
    instrument = data['instrument']
    start_time = int(data["start_time"])
    multiple_prices = retrieve_prices_from_dynamodb(instrument, start_time)
    daily_prices = aggregate_to_day_based_prices(multiple_prices)
    write_daily_prices(daily_prices)
    
def retrieve_prices_from_dynamodb(instrument: str, start_time: int) -> dict: 
    end_time = int(datetime.now().timestamp())
    try:
        table = dynamodb.Table(source_table)
        items = []
        response = table.query(
            KeyConditionExpression=Key('Instrument').eq(instrument) & Key('UnixDateTime').between(start_time, end_time),
            ProjectionExpression='Instrument,UnixDateTime,Price'
        )
        items += response['Items']
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=Key('Instrument').eq(instrument) & Key('UnixDateTime').between(start_time, end_time),
                ProjectionExpression='Instrument,UnixDateTime,Price',
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            items += response['Items']
        return items

    except Exception as e:
        # log the error message
        logger.error("Error occurred while retrieving prices from DynamoDB", e)
        raise ValueError("Error occurred while retrieving prices from DynamoDB")

def aggregate_to_day_based_prices(multiple_prices: dict) -> dict:
    df = pd.DataFrame.from_dict(multiple_prices)
    series = df[['UnixDateTime','Price','Instrument']].copy()
    series['UnixDateTime'] = pd.to_datetime(pd.to_numeric(series['UnixDateTime']), unit='s')
    series.set_index('UnixDateTime', inplace=True)
    series.index.name = 'DateTime'
    series['Price'] = pd.to_numeric(series['Price'])
    daily_summary = series.resample('D').agg({'Price': 'mean', 'Instrument': 'first'})
    daily_summary.reset_index(inplace=True)
    daily_summary['UnixDateTime'] = daily_summary['DateTime'].apply(lambda x: int(x.timestamp()))
    
    dailyPrices= daily_summary.drop(columns=['DateTime'], axis=1)
    cleanedData = dailyPrices.dropna()
    return cleanedData.to_dict('records')

def write_daily_prices(dailyPrices: dict):
    # initiate batch_items
    batch_items = []
    copied_items = 0
    for dailyPrice in dailyPrices:
        dailyPrice['Price'] = Decimal(str(dailyPrice['Price']))
        batch_items.append({'PutRequest': {'Item': dailyPrice}})
        copied_items += 1
        # write to destination
        dynamodb.batch_write_item(RequestItems={target_table: batch_items})
        # reset the batch_items after writting
        batch_items = []

    # final write if any items remaining
    if len(batch_items) > 0:
        dynamodb.batch_write_item(RequestItems={target_table: batch_items})
        copied_items += len(batch_items)

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_REST)
def lambda_handler(event: dict, context: LambdaContext) -> dict:
    return app.resolve(event, context)