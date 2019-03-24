import boto3


def create_anonymized_bucket(bucket_name: str) -> str:

    """
    Creates new buckets in which the anonymized data will be stored

    :param bucket_name: name of the bucket receiving valid events
    :return: list containing valid and invalid bucket responses
    """

    # Create s3 session and retrieve current region
    print(f"Creating S3 client ...")
    s3_connection = boto3.client('s3')
    session = boto3.session.Session()
    current_region = session.region_name

    print(f"Creating new bucket {bucket_name} for storing anonymized data...")
    response = s3_connection.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={
            'LocationConstraint': current_region
        }
    )

    return response
