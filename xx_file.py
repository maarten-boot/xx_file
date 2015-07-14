# ! /usr/bin/env python
# ============================
# ============================

__author__ 'maarten.boot'

VERBOSE = True

# 5 MB
BLOCK_BYTES = 1024 * 1024 * 5

# python built in
WITH_HASH_MD5 = True

# python built in
WITH_HASH_SHA1 = True

# python built in
WITH_COMPRESS_GZIP = True

# python built in
WITH_COMPRESS_BZ2 = True

# easy_install pylzma
WITH_COMPRESS_LZMA = True

COMPRESS_LEVEL = 9

import gc
import mmap
import sys
import time
import hashlib

if WITH_COMPRESS_GZIP:
    import zlib

if WITH_COMPRESS_BZ2:
    import bz2

if WITH_COMPRESS_LZMA:
    import pylzma

# ============================
# ============================

# calulate the block checksums: md5
def block_hash_md5(block, f_block):
    x_md5 = hashlib.md5()
    x_md5.update(block)
    f_block['md5'] = x_md5.hexdigest()


# calulate the block checksums: sha1
def block_hash_sha1(block, f_block):
    x_sha1 = hashlib.sha1()
    x_sha1.update(block)
    f_block['sha1'] = x_sha1.hexdigest()


def block_compres_gzip(level, block, f_block):
    z_data = zlib.compress(block, level)
    z_data_len = len(z_data)
    f_block['gzip_len'] = z_data_len

    return z_data


def block_compres_bz2(level, block, f_block):
    z_data = bz2.compress(block, level)
    z_data_len = len(z_data)
    f_block['bz2_len'] = z_data_len

    return z_data


def block_compres_lzma(level, block, f_block):
    z_data = pylzma.compress(block, level)
    z_data_len = len(z_data)
    f_block['lzma_len'] = z_data_len

    return z_data

class XX_FILE:

    def __init__(self,f_path,block_bytes):
        self.f_path = f_path

        self.offset = 0
        self.blen = 0
        self.size = 0
        self.block_bytes = block_bytes

        self.t_01 = time.time()
        self.t_02 = None

        self.level = COMPRESS_LEVEL

        self.data = None
        self.z_data = None

        self.f = None
        self.mm = None

        self.f_struct = {}
        self.x_f = {}
        

    def xx_file_pre(self):
        self.mm = mmap.mmap(self.f.fileno(), 0, prot=mmap.PROT_READ)
        self.size = self.mm.size()
        self.mm.seek(self.offset)

        if WITH_HASH_MD5:
            self.f_struct['md5'] = None
            self.x_f['md5'] = hashlib.md5()

        if WITH_HASH_SHA1:
            self.f_struct['sha1'] = None
            self.x_f['sha1'] = hashlib.sha1()

        self.f_struct['name'] = self.f_path
        self.f_struct['size'] = self.size
        self.f_struct['blocks'] = []
        self.f_struct['block_count'] = 0
        self.f_struct['block_size'] = self.block_bytes
        self.f_struct['block_compress'] = None
        self.f_struct['hashes'] = self.x_f.keys()


    def xx_file_post(self):
        self.size_post = self.mm.size()

        if self.size_post != self.size:
            file_changed_size = self.size_post
            print 'WARN: file changed size during processing from %d to %d' % (
                self.size,
                self.file_changed_size
            )

        self.mm.close()

        # ==================================
        # ==================================

        if WITH_HASH_MD5:
            self.f_struct['md5'] = self.x_f['md5'].hexdigest()

        if WITH_HASH_SHA1:
            self.f_struct['sha1'] = self.x_f['sha1'].hexdigest()

        self.t_02 = time.time()
        t_0_diff = self.t_02 - self.t_01

        if VERBOSE:
            print 'Time:', t_0_diff, self.f_struct


    def xx_file(self):

        with open(self.f_path, 'rb') as self.f:
            self.xx_file_pre()

            self.n = 0
            while True:
                self.n += 1

                f_block = self.xx_file_block()
                if f_block is None:
                    self.n -= 1  # fix the counter
                    break

                self.f_struct['blocks'].append(f_block)
                self.f_struct['block_count'] = self.n

            self.xx_file_post()

    def xx_file_block(self):
        t1 = time.time()
        gc.collect()

        self.data = self.mm.read(self.block_bytes)
        self.blen = len(self.data)

        if self.blen == 0:
            return None

        # update the file status
        if WITH_HASH_MD5:
            self.x_f['md5'].update(self.data)

        if WITH_HASH_SHA1:
            self.x_f['sha1'].update(self.data)

        # ==================================
        # ==================================

        f_block = {'number': self.n, 'len': self.blen}

        if WITH_HASH_MD5:
            block_hash_md5(self.data, f_block)

        if WITH_HASH_SHA1:
            block_hash_sha1(self.data, f_block)

        # ==================================
        # ==================================
        z_data = None
        if WITH_COMPRESS_GZIP:
            z_data = block_compres_gzip(self.level, self.data, f_block)

        z2_data = None
        if WITH_COMPRESS_BZ2:
            z2_data = block_compres_bz2(self.level, self.data, f_block)

        lzma_data = None
        if WITH_COMPRESS_LZMA:
            lzma_data = block_compres_lzma(self.level, self.data, f_block)

        # report on the spallest block
        x_len = self.blen
        self.x_compress = 'none'

        if WITH_COMPRESS_GZIP and len(z_data) < x_len:
            x_len = len(z_data)
            self.x_compress = 'gzip'
            self.z_data = z_data

        if WITH_COMPRESS_BZ2 and len(z2_data) < x_len:
            x_len = len(z_data)
            self.x_compress = 'bz2'
            self.z_data = z2_data

        if WITH_COMPRESS_LZMA and len(lzma_data) < x_len:
            x_len = len(z_data)
            self.x_compress = 'lzma'
            self.z_data = lzma_data

        t2 = time.time()
        t_diff = t2 - t1
        f_block['time'] = t_diff

        x_string = 'no compression'
        if self.x_compress != 'none':
            x_string = 'selected compression: %s size original: %d ; size compressed: %d, diff: %d' % (
                self.x_compress,
                self.blen,
                x_len,
                self.blen - x_len
            )

        if VERBOSE:
            print self.n, t_diff, x_string

        sys.stdout.flush()

        return f_block


# ============================
# ============================

if __name__ == '__main__':
    f_path = '/home/mboot/Downloads/ISO/Windows_7_AIO_SP1_x64_x86.ISO'
    f_path = '/home/mboot/Downloads/ISO/CentOS-6.6-x86_64-minimal.iso'

    block_bytes = BLOCK_BYTES
    xx = XX_FILE(f_path, block_bytes)
    xx.xx_file()

# md5sum  Windows_7_AIO_SP1_x64_x86.ISO 4a956d4266d8c925accd145f1613e41d
# sha1sum Windows_7_AIO_SP1_x64_x86.ISO 9e89c46f83ffb5d7e9d9d5b562686fdd3fd281f5

# md5sum  /home/mboot/Downloads/ISO/CentOS-6.6-x86_64-minimal.iso eb3c8be6ab668e6d83a118323a789e6c
# sha1sum /home/mboot/Downloads/ISO/CentOS-6.6-x86_64-minimal.iso 4ed6c56d365bd3ab12cd88b8a480f4a62e7c66d2
