from hashlib import sha256

__author__ = 'posjon97'


def hash_contents(filename):
    with open(filename, 'rb') as f:
        hash_ = sha256()
        for chunk in iter(lambda: f.read(4096), b''):
            hash_.update(chunk)
        return hash_.hexdigest()
