import os

import boto
from boto.s3.connection import OrdinaryCallingFormat


def percent_callback(num_bytes, total_bytes):
    print('\b'*10, '{:.2f}%'.format(num_bytes / total_bytes * 100), sep='', end='', flush=True)


class S3(object):
    def __init__(self, options):
        self.options = options
        self.conn = boto.connect_s3(
            aws_access_key_id=options.access_key,
            aws_secret_access_key=options.secret_key,
            host=options.host,
            is_secure=options.is_secure,
            calling_format=OrdinaryCallingFormat())
        self.bucket = self.ensure_bucket(options.bucket)
        self.default_policy = getattr(options, 'default_policy', 'public-read')

    def ensure_bucket(self, bucket=None):
        if bucket:
            if not isinstance(bucket, str):
                return bucket
            b = self.conn.lookup(bucket)
            if not b:
                b = self.conn.create_bucket(bucket)
            return b
        return self.bucket

    def make_key(self, name, bucket=None):
        bucket = self.ensure_bucket(bucket)
        return bucket.new_key(name)

    def put_filename(self, filename, key_name, bucket=None, metadata=None, **kwargs):
        bucket = self.ensure_bucket(bucket)
        if not metadata:
            metadata = {}
        key = self.make_key(key_name, bucket)
        for k, v in metadata.items():
            key.set_metadata(k, v)
        if not key.exists():
            key.set_contents_from_filename(filename, policy=self.default_policy, **kwargs)
        return key.generate_url(0, query_auth=False)

    def put_string(self, data, key_name, bucket=None, metadata=None, **kwargs):
        bucket = self.ensure_bucket(bucket)
        if not metadata:
            metadata = {}
        key = self.make_key(key_name, bucket)
        for k, v in metadata.items():
            key.set_metadata(k, v)
        if not key.exists():
            key.set_contents_from_string(data, policy=self.default_policy, **kwargs)
        return key.generate_url(0, query_auth=False)

    def delete(self, key_name, bucket=None):
        bucket = self.ensure_bucket(bucket)
        key = bucket.get_key(key_name)
        key.delete()


def main():
    from ingest.mediainfo import get_info
    from ingest.hash import hash_contents
    from ingest.mime import get_mime
    import argparse
    from ingest.envdefault import EnvDefault
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    parser.add_argument('-b', '--bucket', action=EnvDefault, envvar='S3_BUCKET')
    parser.add_argument('-s', '--secret-key', action=EnvDefault, envvar='S3_SECRET_KEY')
    parser.add_argument('-a', '--access-key', action=EnvDefault, envvar='S3_ACCESS_KEY')
    parser.add_argument('--is-secure', action=EnvDefault, required=False, default=False, envvar='S3_HOST_SSL')
    parser.add_argument('-H', '--host', action=EnvDefault, envvar='S3_HOST')
    args = parser.parse_args()
    hash_ = hash_contents(args.filename)
    info = get_info(args.filename)
    info['hash'] = hash_
    if 'mimetype' not in info:
        info['mimetype'] = get_mime(args.filename)
    s3 = S3(args)
    key = os.path.basename(args.filename)
    url = s3.put_filename(args.filename, key, num_cb=100, cb=percent_callback)
    print('\n', url)


if __name__ == '__main__':
    main()
