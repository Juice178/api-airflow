import s3fs
import boto3 

from datetime import datetime

def get_s3_path_of_tody():
    dt_y = datetime.now().year
    dt_m = datetime.now().month
    dt_d = datetime.now().day
   

def write_df_to_s3(df, dst_s3, aws_secret):
    """
    Write a dataframe to s3
    """
    # df.to_csv(outpath, index=False, encoding='utf-8')
    bytes_to_write = df.to_csv(None, index=False).encode("utf-8")
    fs = s3fs.S3FileSystem(key=aws_secret['access_key'], secret=aws_secret['secret_key'])
    print("write a file to s3 with encoding")
    with fs.open(dst_s3, 'wb',  encoding='utf-8') as f:
      f.write(bytes_to_write)