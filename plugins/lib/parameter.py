import boto3
import json


def get_parameter(ssm_name: str):
    """
    Get ssm parameter as a dictionary.
    """
    client = boto3.client('ssm', region_name="ap-northeast-1")

    response = client.get_parameter(
        Name=ssm_name,
        WithDecryption=True
    )

    aws_access_key = json.loads(response['Parameter']['Value'])
    return aws_access_key


if __name__ == "__main__":
    aws_access_key = get_parameter('airflow-s3')
    print(f"response: {aws_access_key}")