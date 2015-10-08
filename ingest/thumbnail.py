from gevent.subprocess import check_output, DEVNULL
from io import BytesIO
from PIL import Image
from base64 import b64encode


def get_thumbs(filename, size=(300, 300)):
    orig_thumb = BytesIO(check_output(['ffmpegthumbnailer', '-i', filename, '-c', 'png', '-o', '-', '-s', '0'], stderr=DEVNULL))
    thumb = BytesIO()
    im = Image.open(orig_thumb)
    im.thumbnail(size)
    im.save(thumb, format='PNG')
    return orig_thumb.getvalue(), thumb.getvalue()

