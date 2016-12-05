from hashlib import sha512

__author__ = 'posjon97'


def hash_contents(filename):
    with open(filename, 'rb') as f:
        hash_ = sha512()
        for chunk in iter(lambda: f.read(65536), b''):
            hash_.update(chunk)
        return hash_.hexdigest()
