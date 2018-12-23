import hashlib
import zlib

def crc32(fname):
    value = 0
    with open(fname,"rb") as f:
        for line in f:
            value = zlib.crc32(line, value)
    return str(value & 0xFFFFFFFF)

def md5(fname):
    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)
    return hash_md5.hexdigest()

if __name__ == '__main__':
    print((crc32('/Users/varin/.bashrc')))
    print((md5('/Users/varin/.bashrc')))