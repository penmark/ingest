from gevent import monkey
monkey.patch_all()
from subprocess import check_output
from lxml import etree
from datetime import datetime
import base64
import os


def mapping(tag_name):
    return {
        'internet_media_type': 'mimetype'
    }.get(tag_name, tag_name)


def get_info(filename):
    output = check_output(['mediainfo', '-f', '--Output=XML', filename])
    stat = os.stat(filename)
    info = dict(file_modified=datetime.fromtimestamp(stat.st_mtime), tracks=[])
    xml = etree.fromstring(output)
    for track in xml.iter('track'):
        track_type = track.attrib['type'].lower()
        if track_type == 'general':
            subsection = info
        else:
            subsection = dict(track=track_type)
            info['tracks'].append(subsection)
        for value in track:
            tag = mapping(value.tag.lower())
            if tag not in subsection:
                if tag == 'cover_data':
                    value = base64.b64decode(value.text)
                else:
                    value = value.text
                subsection[tag] = value

    info['complete_name'] = filename
    info.setdefault('title', info.get('file_name', info.get('complete_name')))
    return info

