import hashlib
import logging

class HashFunction:
    def __init__ (self, bits_hash):
        self.bits_hash = bits_hash

    def hash_func (self, id):
        logging.debug ("HashGenerator::hash_func")

        # first get the digest from hashlib and then take the desired number of bytes from the
        # lower end of the 256 bits hash. Big or little endian does not matter.
        hash_digest = hashlib.sha256 (bytes (id, "utf-8")).digest ()  # this is how we get the digest or hash value
        # figure out how many bytes to retrieve
        num_bytes = int(self.bits_hash/8)  # otherwise we get float which we cannot use below
        hash_val = int.from_bytes (hash_digest[:num_bytes], "big")  # take lower N number of bytes

        return hash_val