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
import sys
import os


class IngestWorker(object):
    def __init__(self, queue, config):
        self.queue = queue
        self.config = config
        self.mongo = Mongo(config.db_url, config.collection)

    def __call__(self):
        while not self.queue.empty():
            filename = self.queue.get()
            filename = os.path.abspath(filename.strip())
            if os.path.isdir(filename):
                continue
            t1 = datetime.now()
            print(filename, '...', sep='')
            sha256 = spawn(hash_contents, filename)
            info = spawn(get_info, filename)
            existing = self.mongo.exists(filename)
            magic_mime = get_mime(filename)
            thumbs = None
            info = info.get()
            if 'video' in magic_mime and (not existing or 'thumbs' not in existing):
                thumbs = spawn(get_thumbs, filename)
            if 'mimetype' not in info:
                info['mimetype'] = magic_mime
            if thumbs is not None:
                orig, small = thumbs.get()
                info['thumbs'] = dict(large=orig, small=small)
            info['sha256'] = sha256.get()
            info['type'] = info['mimetype'].split('/')[0]
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


def ingest(files, config):
    queue = Queue()
    pool = Pool(5)
    boss = spawn(spinup, queue, files)
    for _ in range(5):
        pool.spawn(IngestWorker(queue, config))
    boss.join()
    pool.join()


if __name__ == '__main__':
    from_cmd_line()
