from gevent import monkey, spawn
monkey.patch_all()
from datetime import datetime
from gevent.queue import Queue
from gevent.pool import Pool
from argparse import ArgumentParser
from ingest.hash import hash_contents
from ingest.mediainfo import get_info
from ingest.mime import get_mime
from ingest.mongo import Mongo
from ingest.thumbnail import get_thumbs
from ingest.s3 import s3_import, s3_import_data
import sys
import os


class IngestWorker(object):
    def __init__(self, queue, mongo, s3options=None):
        self.queue = queue
        self.mongo = mongo
        self.s3options = s3options

    def __call__(self):
        while not self.queue.empty():
            filename = self.queue.get()
            filename = os.path.abspath(filename.strip())
            if os.path.isdir(filename):
                continue
            t1 = datetime.now()
            print(filename, '...', sep='')
            existing = self.mongo.exists(filename)
            sha512 = spawn(hash_contents, filename)
            info = spawn(get_info, filename)
            magic_mime = get_mime(filename)
            thumbs = None
            info = info.get()
            if 'video' in magic_mime and (not existing or 'thumbs' not in existing):
                thumbs = spawn(get_thumbs, filename)
            if 'mimetype' not in info:
                info['mimetype'] = magic_mime
            info['sha512'] = sha512.get()
            if thumbs is not None:
                large, small = thumbs.get()
                if self.s3options:
                    thumbopts = self.s3options.copy()
                    thumbopts['bucket'] = 'thumb-lg'
                    large = s3_import_data(large, info, **thumbopts)
                    thumbopts['bucket'] = 'thumb-sm'
                    small = s3_import_data(small, info, **thumbopts)
                info['thumbs'] = dict(large=large, small=small)
            info['type'] = info['mimetype'].split('/')[0]
            if self.s3options:
                info['s3uri'] = s3_import(info, **self.s3options)
            if existing:
                info.pop('title')
                self.mongo.update(info)
            else:
                self.mongo.insert(info)
            print(filename, 'done in {:.2f}s'.format((datetime.now() - t1).total_seconds()))


def from_cmd_line():
    parser = ArgumentParser()
    parser.add_argument('-d', '--db-url', required=True, help='Mongodb url')
    parser.add_argument('-c', '--collection', required=True, help='Mongodb collection')
    parser.add_argument('-b', '--bucket', help='S3 bucket')
    parser.add_argument('-a', '--access-key-id', help='S3 access key id')
    parser.add_argument('-s', '--secret-key', help='S3 secret key')
    parser.add_argument('-H', '--s3host', help='S3 host')
    parser.add_argument('files', help='Files to process', nargs='*')
    args = parser.parse_args()

    if args.files:
        files = args.files
        if files[0] == '-':
            files = sys.stdin
    else:
        files = sys.stdin
    if args.bucket:
        s3options = dict(
                bucket=args.bucket,
                access_key=args.access_key_id,
                secret_key=args.secret_key,
                host=args.s3host)
    else:
        s3options = None
    ingest(files, args.db_url, args.collection, s3options)


def spinup(queue, filenames):
    for filename in filenames:
        queue.put(filename)


def ingest(files, db_url, collection_name, s3options=None):
    queue = Queue()
    pool = Pool(5)
    boss = spawn(spinup, queue, files)
    mongo = Mongo(db_url, collection_name)
    for _ in range(5):
        worker = IngestWorker(queue, mongo, s3options)
        pool.spawn(worker)
    boss.join()
    pool.join()


if __name__ == '__main__':
    from_cmd_line()

