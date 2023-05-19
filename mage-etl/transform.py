import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    RATE_CODE_ID = {
        1: 'Standard rate',
        2: 'JFK',
        3: 'Newark',
        4: 'Nassau or Westchester',
        5: 'Negotiated fare',
        6: 'Group ride'
    }
    PAYMENT_TYPE_ID = {
        1: 'Credit card',
        2: 'Cash',
        3: 'No charge',
        4: 'Dispute',
        5: 'Unknown',
        6: 'Voided trip',
    }

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    datetime_df = df[['tpep_pickup_datetime', 'tpep_dropoff_datetime']].drop_duplicates().reset_index()
    datetime_dim = pd.DataFrame()
    datetime_dim['datetime_id'] = datetime_df.index
    datetime_dim['tpep_pickup_datetime'] = datetime_df['tpep_pickup_datetime']
    datetime_dim['tpep_dropoff_datetime'] = datetime_df['tpep_dropoff_datetime']
    datetime_dim['pick_hour'] = datetime_df['tpep_pickup_datetime'].dt.hour
    datetime_dim['pick_day'] = datetime_df['tpep_pickup_datetime'].dt.day
    datetime_dim['pick_month'] = datetime_df['tpep_pickup_datetime'].dt.month
    datetime_dim['pick_year'] = datetime_df['tpep_pickup_datetime'].dt.year
    datetime_dim['pick_week'] = datetime_df['tpep_pickup_datetime'].dt.isocalendar().week
    datetime_dim['drop_hour'] = datetime_df['tpep_dropoff_datetime'].dt.hour
    datetime_dim['drop_day'] = datetime_df['tpep_dropoff_datetime'].dt.day
    datetime_dim['drop_month'] = datetime_df['tpep_dropoff_datetime'].dt.month
    datetime_dim['drop_year'] = datetime_df['tpep_dropoff_datetime'].dt.year
    datetime_dim['drop_week'] = datetime_df['tpep_dropoff_datetime'].dt.isocalendar().week

    pickup_location_df = df[['pickup_longitude', 'pickup_latitude']].drop_duplicates().reset_index()
    pickup_location_dim = pd.DataFrame()
    pickup_location_dim['pickup_location_id'] = pickup_location_df.index
    pickup_location_dim['pickup_longitude'] = pickup_location_df['pickup_longitude']
    pickup_location_dim['pickup_latitude'] = pickup_location_df['pickup_latitude']

    dropoff_location_df = df[['dropoff_longitude', 'dropoff_latitude']].drop_duplicates().reset_index()
    dropoff_location_dim = pd.DataFrame()
    dropoff_location_dim['dropoff_location_id'] = dropoff_location_df.index
    dropoff_location_dim['dropoff_longitude'] = dropoff_location_df['dropoff_longitude']
    dropoff_location_dim['dropoff_latitude'] = dropoff_location_df['dropoff_latitude']

    passenger_count_df = df[['passenger_count']].drop_duplicates().reset_index()
    passenger_count_dim = pd.DataFrame()
    passenger_count_dim['passenger_count_id'] = passenger_count_df.index
    passenger_count_dim['passenger_count'] = passenger_count_df['passenger_count']

    trip_distance_df = df[['trip_distance']].drop_duplicates().reset_index()
    trip_distance_dim = pd.DataFrame()
    trip_distance_dim['trip_distance_id'] = trip_distance_df.index
    trip_distance_dim['trip_distance'] = trip_distance_df['trip_distance']

    rate_code_df = df[['RatecodeID']].drop_duplicates().reset_index()
    rate_code_dim = pd.DataFrame()
    rate_code_dim['rate_code_id'] = rate_code_df.index
    rate_code_dim['RatecodeID'] = rate_code_df['RatecodeID']
    rate_code_dim['rate_code_name'] = rate_code_df['RatecodeID'].map(RATE_CODE_ID)

    payment_type_df = df[['payment_type']].drop_duplicates().reset_index()
    payment_type_dim = pd.DataFrame()
    payment_type_dim['payment_type_id'] = payment_type_df.index
    payment_type_dim['payment_type'] = payment_type_df['payment_type']
    payment_type_dim['payment_type_name'] = payment_type_df['payment_type'].map(PAYMENT_TYPE_ID)

    fact_table = df.merge(payment_type_dim, on='payment_type') \
            .merge(rate_code_dim, on='RatecodeID') \
            .merge(trip_distance_dim, on='trip_distance') \
            .merge(passenger_count_dim, on='passenger_count') \
            .merge(dropoff_location_dim, on=['dropoff_longitude', 'dropoff_latitude']) \
            .merge(pickup_location_dim, on=['pickup_longitude', 'pickup_latitude']) \
            .merge(datetime_dim, on=['tpep_pickup_datetime', 'tpep_dropoff_datetime']) \
            [['VendorID', 'datetime_id', 'passenger_count_id',
             'trip_distance_id',
             'rate_code_id', 'payment_type_id', 'fare_amount',
             'extra', 'mta_tax', 'tip_amount',
             'tolls_amount', 'improvement_surcharge', 'total_amount',
              'pickup_location_id', 'dropoff_location_id'
            ]]

    return {
        "datetime_dim": datetime_dim.to_dict(orient='dict'),
        "passenger_count_dim": passenger_count_dim.to_dict(orient='dict'),
        "trip_distance_dim": trip_distance_dim.to_dict(orient='dict'),
        "rate_code_dim": rate_code_dim.to_dict(orient='dict'),
        "payment_type_dim": payment_type_dim.to_dict(orient='dict'),
        "pickup_location_dim": pickup_location_dim.to_dict(orient='dict'),
        "dropoff_location_dim": dropoff_location_dim.to_dict(orient='dict'),
        "fact_table": fact_table.to_dict(orient='dict'),
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
