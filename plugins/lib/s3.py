import s3fs
import boto3 

def write_df_to_s3(df, outpath, aws_secret):
    """
    Write a dataframe to s3
    """
    bytes_to_write = df.to_csv(None, index=False).encode()
    fs = s3fs.S3FileSystem(key=aws_secret['key'], secret=aws_secret['secret'])
    with fs.open(outpath, 'wb') as f:
      f.write(bytes_to_write)