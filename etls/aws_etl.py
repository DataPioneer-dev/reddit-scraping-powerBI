import s3fs

from utils.constants import AWS_ACCESS_KEY_ID, AWS_ACCESS_KEY


def connect_to_s3():
    try:
        s3 = s3fs.S3FileSystem(anon=False, key=AWS_ACCESS_KEY_ID, secret=AWS_ACCESS_KEY)
        return s3

    except Exception as e:
        print(e)

def create_bucket(s3, bucket):
    try:
        if s3.exists(bucket):
            s3.mkdir(bucket)
            print("Bucket Created")
        else:
            print("Bucket already created")

    except Exception as e:
        print(e)

def upload_to_s3(s3, file_path, bucket, file_name):
    try:
        s3.put(file_path, bucket+'/raw/'+file_name)
        print("File uploaded to s3")
    except FileNotFoundError:
        print("File not fount")

