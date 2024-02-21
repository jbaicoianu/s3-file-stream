# S3 File Stream

This library exposes drop-in replacements for a number of common file and directory operations which makes it easy to adapt existing libraries to stream data to and from S3 without rewriting existing filesystem-based code.

# Examples
## Reading:
```
from s3filestream import open

with open('s3://my-test-bucket/path/to/file') as fd:
    for line in fd:
        # do something with line
        print(line)
```

## Writing:
```
from s3filestream import open

filepath = 's3://my-test-bucket/path/to/file'
with open(filepath, 'w') as fd:
    for i in range(1000):
        fd.write(f'{i}\n'.encode('utf-8'))
```

## Iterating files in a directory:
```
from s3filestream import open, listdir, isfile, path_exists

def list_dir_recursive(dirname):
    for filename in listdir(dirname):
        filepath = basedir + filename
        if isdir(filepath):
            # Recurse into directories
            list_dir(filepath)
        else:
            # Print file contents
            with open(filepath) as fd:
                print(filepath + ':')
                for line in fd:
                    print(line)

basedir = 's3://my-test-bucket/path/to/directory/'

if path_exists(basedir):
    list_dir_recursive(basedir
```

## Extracting files from a tar file:
```
from s3filestream import open
import tarfile
import random

with open('s3://my-test-bucket/path/to/test-data.tar', 'rb') as fd:
    with tarfile.open(fileobj=fd) as tardata:
        names = tardata.getnames()
        print(names)
        # Extract and print a random file
        compressedfile = tardata.extractfile(random.choice(names))
        print(compressedfile)
```


# Performance Caveats
Be aware that the performance characteristics for S3 differ greatly from the same operations on a local or distributed filesystem, and tuning for your use case will likely be necessary. 
 * S3 is a high latency, medium throughput storage service. It can deliver data at a rate in the hundreds of megs per second, but sometimes reads take upwards of 10 seconds to begin. Increasing parallelism in your app can help here.
 * This library works best for sequential reads, and implements chunked read-ahead. Random access is supported, but is suboptimal for very large files. Small to medium files will buffer in memory and random reads will be lightning fast, but for larger files the block fetching takes long enough to significantly impact performance
