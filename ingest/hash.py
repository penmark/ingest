from hashlib import md5


def hash_contents(filename):
    with open(filename, 'rb') as f:
        hash_ = md5()
        for chunk in iter(lambda: f.read(65536), b''):
            hash_.update(chunk)
        return hash_.hexdigest()
