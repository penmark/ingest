# ingest
Python media ingester for MongoDB

## .env
If present, environment variables will be read from .env file (see [python-dotenv](https://github.com/theskumar/python-dotenv/blob/master/README.rst)).

## Environment variables
```
MONGO_URI # eg mongodb://localhost/media
MONGO_COLLECTION # eg asset
S3_BUCKET # bucket name
S3_ACCESS_KEY # aws/ceph s3 key
S3_SECRET_KEY # aws/ceph s3 secret key
S3_SSL # use ssl? default true
S3_HOST # for ceph S3, not needed for AWS S3
S3_CALLING_FORMAT # for ceph S3 use "boto.s3.connection.OrdinaryCallingFormat", not needed for AWS S3 (or use "boto.s3.connection.SubdomainCallingFormat")
```
