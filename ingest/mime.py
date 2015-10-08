from magic import Magic


def get_mime(filename):
    f = Magic(mime=True)
    return f.from_file(filename).decode()
