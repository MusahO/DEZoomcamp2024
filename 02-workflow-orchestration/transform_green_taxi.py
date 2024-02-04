if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    column_name_mapping = {
        'VendorID': 'vendor_id',
        'RatecodeID': 'ratecode_id',
        'PULocationID': 'pu_location_id',
        'DOLocationID': 'do_location_id',
    }
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]

    data["lpep_pickup_date"] = data.lpep_pickup_datetime.dt.date

    data = data.rename(columns=column_name_mapping)
    print(data['vendor_id'].value_counts())

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
    # assert 'vendor_id' in output.columns, 'vendor_id column is missing'
    # assert all(output['passenger_count'] > 0),  'passenger_count columns contains zero passengers'
    # assert all(output['trip_distance'] > 0),  'trip_distance columns contains zero trips'

@test
def test_output_vendor_id(output, *args) -> None:
    """
    Template code for testing the vendor id column exists in output.
    """
    assert 'vendor_id' in output.columns, 'vendor_id column is missing'

@test
def test_output_passenger_count(output, *args) -> None:
    """
    Template code for testing the passenger count > 0.
    """
    assert all(output['passenger_count'] > 0),  'passenger_count columns contains zero passengers'

@test
def test_output_trip_distance_count(output, *args) -> None:
    """
    Template code for testing the trip distance > 0.
    """
    assert all(output['trip_distance'] > 0),  'trip_distance columns contains zero trips distance'
