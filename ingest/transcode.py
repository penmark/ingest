from gevent import monkey, spawn
monkey.patch_all()
from gevent.subprocess import Popen, PIPE, DEVNULL, check_output
from gevent.queue import Queue
from datetime import timedelta
import sys
import io
import json


def get_duration(filename):
    ffprobe = ['ffprobe', '-v', 'error', '-show_format',
               '-print_format', 'json', filename]
    data = json.loads(check_output(ffprobe).decode())
    return float(data['format']['duration']) * 1e6


def transcode(infile, outfile, metadata=None, done_callback=None, progress_callback=None):
    ffmpeg = ['ffmpeg', '-hide_banner', '-loglevel', 'warning', '-y',
              '-progress', '-', '-i', infile, '-codec:v', 'h264',
              '-codec:a', 'aac', '-strict', '-2', outfile]
    if metadata:
        for key, val in metadata.items():
            ffmpeg[1:1] = ['-metadata', '{}={}'.format(key, value)]
    out = PIPE if progress_callback else DEVNULL
    with Popen(ffmpeg, stdout=out) as process:
        if not progress_callback:
            process.wait()
            return
        duration = get_duration(infile)
        while process.poll() is None:
            for line in iter(process.stdout.readline, b''):
                if b'out_time_ms' in line:
                    out_time_ms = int(line[12:])
                    percent = out_time_ms / duration * 100
                    progress_callback(percent)
            progress_callback(100)
    done_callback()


if __name__ == '__main__':
    infile, outfile = sys.argv[1:3]
    def progress_callback(percent):
        print('{}%'.format(percent), end='\n', flush=True)
    def done_callback():
        print('Done')
    print('Transcoding {} to {}...'.format(infile, outfile))
    proc = spawn(transcode, infile, outfile, done_callback, progress_callback)
    try:
        proc.join()
    except:
        proc.kill()

