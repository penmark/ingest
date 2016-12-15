import sys
import os
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
from s3_wrapper import S3


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
            if self.s3 and 'video' in magic_mime:
                thumbs = spawn(get_thumbs, filename)
            if 'mimetype' not in info:
                info['mimetype'] = magic_mime
            info['hash'] = hash_.get()
            if thumbs is not None:
                large, small = thumbs.get()
                key_fmt = 'thumb/{}/{{}}/{}.png'.format(info['hash'], basename)
                large_url = spawn(self.s3.put_string, large, key_fmt.format('lg'))
                small_url = spawn(self.s3.put_string, small, key_fmt.format('sm'))
                info['thumbs'] = dict(large=large_url.get(), small=small_url.get())
            info['type'] = info['mimetype'].split('/')[0]
            if self.s3:
                key = '{}/{}/{}'.format(info['type'], info['hash'], basename)
                metadata = dict(filename=basename, mimetype=info['mimetype'])
                if thumbs:
                    metadata.update(thumbnail=info['thumbs']['small'])

                def progress_callback(num_bytes, total_bytes):
                    progress = '{:s} {:.2f}%'.format(basename, num_bytes / total_bytes * 100)
                    print(progress, sep='', flush=True)
                s3_uri = spawn(self.s3.put_filename, filename, key, metadata=metadata, cb=progress_callback)
                info['s3uri'] = s3_uri.get()
                print('\n', info['s3uri'], sep='')
            if existing:
                self.mongo.update(info)
            else:
                self.mongo.insert(info)
            print(filename, 'done in {:.2f}s'.format((datetime.now() - t1).total_seconds()))


def from_cmd_line():
    from s3_wrapper.envdefault import EnvDefault, truthy
    from dotenv import load_dotenv, find_dotenv

    load_dotenv(find_dotenv(usecwd=True))

    parser = ArgumentParser()
    parser.add_argument('-d', '--db-url', required=True, action=EnvDefault, envvar='MONGO_URI',
                        help='Mongodb url')
    parser.add_argument('-c', '--collection', required=True, action=EnvDefault, envvar='MONGO_COLLECTION',
                        help='Mongodb collection')
    parser.add_argument('-b', '--bucket', action=EnvDefault, envvar='S3_BUCKET',
                        help='S3 bucket')
    parser.add_argument('-a', '--access-key', action=EnvDefault, envvar='S3_ACCESS_KEY',
                        help='S3 access key')
    parser.add_argument('-s', '--secret-key', action=EnvDefault, envvar='S3_SECRET_KEY',
                        help='S3 secret key')
    parser.add_argument('--is-secure', action=EnvDefault, required=False, envvar='S3_SSL', type=truthy, default=False,
                        help='S3 use ssl')
    parser.add_argument('-H', '--host', action=EnvDefault, required=False, envvar='S3_HOST',
                        help='S3 host')
    parser.add_argument('--calling-format', action=EnvDefault, required=False, envvar='S3_CALLING_FORMAT',
                        help='S3 calling format')
    parser.add_argument('files', nargs='*',
                        help='Files to process')
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
    use_s3 = options.bucket and options.access_key and options.secret_key
    if use_s3:
        s3 = S3(options)
    else:
        print('Not using s3')
        s3 = None
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
