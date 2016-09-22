from gevent.subprocess import check_output
from lxml import etree
from datetime import datetime
import base64
import os
import sys
from bson import json_util


class SkipTag(object):
    pass


def tag_mapping(tag_name):
    return {
        'internet_media_type': 'mimetype',
        'file_last_modification_date': SkipTag,
        'file_last_modification_date__local_': SkipTag
    }.get(tag_name, tag_name)


def value_mapping(tag_name):
    return {
        'bit_depth': int,
        'bit_rate': int,
        'count': int,
        'count_of_audio_streams': int,
        'count_of_video_streams': int,
        'count_of_stream_of_this_kind': int,
        'cover_data': base64.b64decode,
        'display_aspect_ratio': float,
        'duration': float,
        'file_size': int,
        'frame_count': int,
        'frame_rate': float,
        'height': int,
        'nominal_bit_rate': int,
        'number_of_frames': int,
        'overall_bit_rate': int,
        'pixel_aspect_ratio': float,
        'proportion_of_this_stream': float,
        'stream_size': int,
        'samples_count': int,
        'width': int,
    }.get(tag_name, str)


def get_info(filename):
    output = check_output(['mediainfo', '-f', '--Output=XML', filename])
    stat = os.stat(filename)
    info = dict(file_modified=datetime.fromtimestamp(stat.st_mtime), tracks=[])
    xml = etree.fromstring(output)
    for track in xml.iter('track'):
        track_type = track.attrib['type'].lower()
        # Put the "general" track in the top level
        if track_type == 'general':
            subsection = info
        else:
            subsection = dict(track=track_type)
            info['tracks'].append(subsection)
        for value in track:
            tag = tag_mapping(value.tag.lower())
            if tag == SkipTag:
                continue
            # Use first tag only.
            # mediatype -f repeats tags; first tag is the one with the best information
            if tag not in subsection:
                try:
                    value = value_mapping(tag)(value.text)
                except ValueError:
                    print('ValueError for {}; {} isn\'t {}:able'.format(tag, value_mapping(tag), value.text))
                    value = value.text
                subsection[tag] = value

    info['complete_name'] = filename
    info.setdefault('title', info.get('file_name', info.get('complete_name')))
    return info


def from_cmd_line():
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('file', nargs='*', default=sys.stdin, help='Mediafile to examine')
    args = parser.parse_args()
    for f in args.file:
        print(json_util.dumps(get_info(f), indent=2))

if __name__ == '__main__':
    from_cmd_line()
