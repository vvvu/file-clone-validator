# file-clone-validator
File migration validation tool

Known issues & possible improvements:
- [] use BadgerDB to store the file meta rather than using the current file format
  - [] the file relative path can be stored as the key
- [] add support for validating files in different granularity
  - [] validate files that only check the file existence (key-only iteration in BadgerDB)
  - [] validate files that only check the file meta (without checking the checksum)
- [] replace json with protoBuf or flatBuffers which is more efficient and support unmarshal partially
- [] add concurrency support by using BadgerDB Stream
