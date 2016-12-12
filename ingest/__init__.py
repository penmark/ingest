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
from ingest.s3 import S3, percent_callback
import sys
import os


class IngestWorker(object):
    def __init__(self, queue, mongo, s3, options):
        self.queue = queue
        self.mongo = mongo
        self.s3 = s3
        self.options = options

    def __call__(self):
        while not self.queue.empty():
            filename = self.queue.get()
            filename = os.path.abspath(filename.strip())
            basename = os.path.basename(filename)
            if os.path.isdir(filename):
                continue
            t1 = datetime.now()
            print(filename, '...', sep='')
            existing = self.mongo.exists(filename)
            hash_ = spawn(hash_contents, filename)
            info = spawn(get_info, filename)
            magic_mime = get_mime(filename)
            thumbs = None
            info = info.get()
            if 'video' in magic_mime:
                thumbs = spawn(get_thumbs, filename)
            if 'mimetype' not in info:
                info['mimetype'] = magic_mime
            info['hash'] = hash_.get()
            if thumbs is not None:
                large, small = thumbs.get()
                key_fmt = 'thumb/{}/{{}}/{}.png'.format(info['hash'], basename)
                large_url = self.s3.put_string(large, key_fmt.format('lg'))
                small_url = self.s3.put_string(small, key_fmt.format('sm'))
                info['thumbs'] = dict(large=large_url, small=small_url)
            info['type'] = info['mimetype'].split('/')[0]
            key = '{}/{}/{}'.format(info['type'], info['hash'], basename)
            metadata = dict(filename=basename, mimetype=info['mimetype'], thumbnail=info['thumbs']['small'])
            info['s3uri'] = self.s3.put_filename(filename, key, metadata=metadata, num_cb=100, cb=percent_callback)
            print('Asset uri:', info['s3uri'])
            if existing:
                info.pop('title')
                self.mongo.update(info)
            else:
                self.mongo.insert(info)
            print(filename, 'done in {:.2f}s'.format((datetime.now() - t1).total_seconds()))


def from_cmd_line():
    from ingest.envdefault import EnvDefault
    parser = ArgumentParser()
    parser.add_argument('-d', '--db-url', required=True, action=EnvDefault, envvar='MONGO_URI', help='Mongodb url')
    parser.add_argument('-c', '--collection', required=True, action=EnvDefault, envvar='MONGO_COLLECTION',
                        help='Mongodb collection')
    parser.add_argument('-b', '--bucket', action=EnvDefault, envvar='S3_BUCKET', help='S3 bucket')
    parser.add_argument('-a', '--access-key', action=EnvDefault, envvar='S3_ACCESS_KEY', help='S3 access key')
    parser.add_argument('-s', '--secret-key', help='S3 secret key')
    parser.add_argument('--is-secure', action=EnvDefault, required=False, default=False, envvar='S3_HOST_SSL')
    parser.add_argument('-H', '--host', action=EnvDefault, envvar='S3_HOST', help='S3 host')
    parser.add_argument('files', help='Files to process', nargs='*')
    args = parser.parse_args()

    if args.files:
        files = args.files
        if files[0] == '-':
            files = sys.stdin
    else:
        files = sys.stdin

    ingest(files, args)


def spinup(queue, filenames):
    for filename in filenames:
        queue.put(filename)


def ingest(files, options):
    mongo = Mongo(options.db_url, options.collection)
    s3 = S3(options)
    queue = Queue()
    pool = Pool(5)
    boss = spawn(spinup, queue, files)
    for _ in range(5):
        worker = IngestWorker(queue, mongo, s3, options)
        pool.spawn(worker)
    boss.join()
    pool.join()


if __name__ == '__main__':
    from_cmd_line()
