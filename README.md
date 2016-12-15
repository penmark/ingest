# ingest
Python media ingester for MongoDB

## .env
If present, environment variables will be read from .env file

## Environment variables
MONGO\_URI - eg mongdb://localhost/media
MONGO\_COLLECTION - eg asset
S3\_BUCKET
S3\_ACCESS\_KEY
S3\_SECRET\_KEY
S3\_SSL - eg true
S3\_HOST - for rados gw, not needed for AWS S3
S3\_CALLING\_FORMAT - for rados gw, not needed for AWS S3

