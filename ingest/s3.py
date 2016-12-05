from boto.s3.connection import OrdinaryCallingFormat
import boto
import os

def connect(access_key, secret_key, host, is_secure=False):
    return boto.connect_s3(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            host=host,
            is_secure=is_secure,
            calling_format=OrdinaryCallingFormat())


def s3_import(info, bucket, access_key, secret_key, host, is_secure=False, callback=None):
    conn = connect(access_key, secret_key, host, is_secure)
    b = conn.lookup(bucket)
    if not b:
        b = conn.create_bucket(bucket)
    key = b.new_key(info['sha512'])
    if key.exists():
        return
    key.set_metadata('filename', os.path.basename(info['complete_name']))
    key.set_metadata('mimetype', info['mimetype'])
    key.set_contents_from_filename(info['complete_name'], policy='public-read', cb=callback)
    return key.generate_url(0, query_auth=False)


def s3_import_data(data, info, bucket, access_key, secret_key, host, is_secure=False):
    conn = connect(access_key, secret_key, host, is_secure)
    b = conn.lookup(bucket)
    if not b:
        b = conn.create_bucket(bucket)
    key = b.new_key(info['sha512'])
    if key.exists():
        return
    key.set_contents_from_string(data, policy='public-read')
    return key.generate_url(0, query_auth=False)


if __name__ == '__main__':
    from ingest.mediainfo import get_info
    from ingest.hash import hash_contents
    from ingest.mime import get_mime
    import sys
    sha512 = hash_contents(sys.argv[1])
    info = get_info(sys.argv[1])
    info['sha512'] = sha512
    if 'mimetype' not in info:
        info['mimetype'] = get_mime(sys.argv[1])
    print(s3_import(info, sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5], callback=lambda l, s: print('{0:.2f}% done'.format(l / s * 100), sep='')))

