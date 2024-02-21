import boto3
import botocore
import io
import threading
import queue
import multiprocessing
from collections import OrderedDict
import os
import time
from pathlib import PosixPath

# Represents an IO stream interface for reading and writing files directly to and from S3
# Intended to be passed in eg, as the fileobj parameter for a tarfile, to stream tar, zips, etc. remotely

# This was partly written with GPT4's help, but GPT4 is terrible at writing threaded code, so I had to heavily modify it anyway
s3 = boto3.client('s3')

class S3FileStream(io.IOBase):
    def __init__(self, path, chunk_size=500 * 1024 * 1024):
        super().__init__()
        self.path = path
        self.chunk_size = chunk_size
        self.bucket, self.key = self._parse_s3_path(path)
        self.s3 = s3 #boto3.client('s3')
        self.file_size = self._get_file_size()
        self.cache = OrderedDict()
        self.offset = 0
        self.current_chunk = 0
        self.next_chunk = 0
        self.chunk_fetched = threading.Event()
        self.write_buffer = io.BytesIO()
        self.write_parts = []
        self.upload_id = None

        self.worker = None
        self.threadpool = None
        #print(' => OPEN ', self.path, self)

        self.read_queue = queue.Queue()
        self.read_ahead = 2

        #self.create_download_worker()

    def _parse_s3_path(self, path):
        if isinstance(path, PosixPath):
            path = str(path)
        assert path.startswith('s3://')
        path = path[len('s3://'):]
        parts = path.split('/', 1)
        return parts[0], parts[1]

    def create_download_worker(self):
        self.worker = threading.Thread(target=self._worker)
        self.worker.start()

    def create_upload_worker(self):
        self.threadpool = multiprocessing.pool.ThreadPool(2)

    def get_chunk(self, chunknum):
        if chunknum in self.cache:
            return self.cache[chunknum]

        # Queue a request to fetch the next chunk
        event = threading.Event()
        self.read_queue.put((chunknum, event))
        #print('blocked reading chunk', chunknum)
        event.wait()
        #print('unblocked')

        self.prune_cache()

        return self.cache[chunknum]

    def read(self, n=-1):
        if not self.worker:
            self.create_download_worker()
        #print('read called', n, self.tell())
        if n == -1:
            n = self.file_size - loc
        result = bytearray()
        while n > 0:
            loc = self.offset
            chunk = loc // self.chunk_size
            offset = loc % self.chunk_size

            data = self.get_chunk(chunk)

            read_size = min(n, len(data) - offset)
            result.extend(data[offset:offset+read_size])
            self.seek(loc + read_size)
            n -= read_size
            if read_size == 0 and n > 0:
                # no data left to read
                break
        return bytes(result)

    def _worker(self):
        num_chunks = (self.file_size // self.chunk_size) + 1

        while True:
            current_chunk = self.tell() // self.chunk_size
            try:
                chunkdata = self.read_queue.get(timeout=.1)
                if not chunkdata: # False encountered, terminate thread
                    #print('Worker thread terminating')
                    return
                if chunkdata[0] in self.cache:
                    if chunkdata[1]:
                        chunkdata[1].set()
                else:
                    data = self.worker_get_chunk(chunkdata[0])
                    self.cache[chunkdata[0]] = data
                    if chunkdata[1]:
                        chunkdata[1].set()

                self.read_queue.task_done()

                # Enqueue up to self.read_ahead chunks once we finish fetching this one
                # FIXME - if fetching is faster than processing, this still causes blocking every self.read_ahead chunks. The readahead logic should really execute intelligently on read(), not after each chunk is fetched like it is here.
                read_ahead = self.read_ahead - self.read_queue.qsize()
                if read_ahead > 0:
                    for i in range(0, read_ahead):
                        next_chunk = chunkdata[0] + i + 1
                        if next_chunk - current_chunk > 0 and next_chunk - current_chunk <= self.read_ahead and next_chunk < num_chunks:
                            self.read_queue.put((next_chunk, None))
            except queue.Empty:
                if self.closed:
                    break


    def worker_get_chunk(self, chunk):
        start = chunk * self.chunk_size
        end = min((chunk + 1) * self.chunk_size - 1, self.file_size)
        #print(f'fetch chunk {chunk} from {self.path} range {start}-{end}', flush=True)
        response = self.s3.get_object(Bucket=self.bucket, Key=self.key, Range=f'bytes={start}-{end}')
        body = response['Body'].read()
        length = len(body)
        #print(f'got chunk {chunk} from {self.path} range {start}-{end} ({length} bytes)')
        return body

    def tell(self):
        return self.offset

    def _get_file_size(self):
        try:
            response = self.s3.head_object(Bucket=self.bucket, Key=self.key)
            return response['ContentLength']
        except botocore.exceptions.ClientError:
            return 0


    def _upload_part(self, part_number, data):
        print('upload part', self.upload_id, part_number, len(data), flush=True)
        try:
            response = self.s3.upload_part(
                Body=data,
                Bucket=self.bucket,
                Key=self.key,
                UploadId=self.upload_id,
                PartNumber=part_number,
            )
            print(response)
            if response['ResponseMetadata']['HTTPStatusCode'] == 200:
                self.write_parts.append({'ETag': response['ETag'], 'PartNumber': part_number})
        except Exception as e:
            print('wtf m8', e)

    def write(self, b):
        if self.closed:
            raise ValueError("I/O operation on closed file")

        if not self.threadpool:
            self.create_upload_worker()

        length = self.write_buffer.write(b)
        self.offset += length

        if self.write_buffer.tell() >= self.chunk_size:
            #part_number = self.offset // self.chunk_size
            #part_number = self.next_chunk
            #self.next_chunk += 1
            #data = self.write_buffer.getvalue()
            #self.threadpool.apply_async(func=self._upload_part, args=(part_number, data))

            # Clear the buffer
            #self.write_buffer = io.BytesIO()
            self.flush()
        return length

    def seek(self, offset, whence=io.SEEK_SET):
        if whence == io.SEEK_SET:
            self.offset = offset
        elif whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.file_size + offset
        else:
            raise ValueError("Invalid value for whence")

        #self.next_chunk = self.offset // self.chunk_size
        return self.offset
    def flush(self):
        if self.write_buffer.tell() > 0:
            #part_number = len(self.write_parts) + 1
            #part_number = self.offset // self.chunk_size + 1
            self.next_chunk += 1
            part_number = self.next_chunk
            data = self.write_buffer.getvalue()

            if not self.upload_id:
                print('no upload id found yet, get one', self.upload_id)
                self.upload_id = self.s3.create_multipart_upload(Bucket=self.bucket, Key=self.key)['UploadId']
                if not self.upload_id:
                    raise Exception(f'Failed to start multipart upload for s3://{self.bucket}/{self.key}, check permissions') 
                print('new upload id: ' + self.bucket, self.key, self.upload_id, self)


            self.threadpool.apply_async(func=self._upload_part, args=(part_number, data))

            # Clear the buffer
            self.write_buffer = io.BytesIO()

    def close(self):
        #if self.closed:
        #    print('already closed')
        #    return
        self.flush()
        if self.worker:
            if self.read_queue.qsize() > 0:
                queue_items = list(self.read_queue.queue)
                for queue_item in queue_items:
                    if queue_item[1]:
                        queue_item[1].wait()
            #print('kill the worker')
            self.read_queue.put(False)
            self.worker.join()
            self.worker = None

        if self.threadpool:
            self.threadpool.close()
            self.threadpool.join()
            #if self.buffer.tell() > 0:
            #    self.flush()
            print('upload finished', self.upload_id, self.bucket, self.key, self.write_parts)
            try:
                parts = sorted(self.write_parts, key=lambda part: part['PartNumber'])
                response = self.s3.complete_multipart_upload(
                    Bucket=self.bucket,
                    Key=self.key,
                    UploadId=self.upload_id,
                    MultipartUpload={'Parts': parts},
                )
                print('Multipart upload completed')
            except Exception as e:
                print('Multipart upload failed: ', str(e), self.bucket)
                print(parts)
            #self.closed = True
        #print(' => CLOSED!', self.path, self, flush=True)
        super().close()
    def prune_cache(self):
        current_chunk = self.tell() // self.chunk_size
        items = self.cache.items()
        deletes = []
        for (f, data) in items:
            if f < current_chunk:
                deletes.append(f)

        if len(deletes) > 0:
            #print('Evicting from chunk cache:', f)
            for f in deletes:
                del self.cache[f]

os_open = open
os_listdir = os.listdir
os_path_isfile = os.path.isfile
os_path_isdir = os.path.isdir
os_path_exists = os.path.exists


def open(path, *args, **kwargs):
    if isinstance(path, PosixPath):
        path = str(path)
    if path.startswith('s3://'):
        return S3FileStream(path, **kwargs)
    else:
        return os_open(path, *args, **kwargs)

def listdir(path='.'):
    if path[0:5] == 's3://':
        files = []
        bucket, key = path[5:].split('/', 1)
        #print('list bucket:', bucket)
        #print('list key:', key)
        #s3 = boto3.client('s3')
        token = None 
        while True:
            if token:
              f = s3.list_objects_v2(Bucket=bucket, Prefix=key, ContinuationToken=token)
            else:
              f = s3.list_objects_v2(Bucket=bucket, Prefix=key)
            for k in f['Contents']:
                #print(k)
                #files.append(os.path.basename(k['Key']))
                files.append(k['Key'][len(key):])
            if not f['IsTruncated']:
                break;
            else:
                token = f['NextContinuationToken']
    else:
        files = os_listdir(path)
    return files

def isfile(path):
    if isinstance(path, PosixPath):
        path = str(path)
    if path[0:5] == 's3://':
        #print('full path', path)
        #s3 = boto3.client('s3')
        bucket, key = path[5:].split('/', 1)
        #print('list bucket:', bucket)
        #print('list key:', key)
        try:
            f = s3.head_object(Bucket=bucket, Key=key)
            #print(f)
            return True
        except:
            pass
        return False
    else:
      return os_path_isfile(path)

def isdir(path):
    if isinstance(path, PosixPath):
        path = str(path)
    if path.startswith('s3://'):
        if path[-1] != '/':
            path += '/'
        #print('is dir?', path)
        #print('full path', path)
        bucket, key = path[5:].split('/', 1)
        #print('list bucket:', bucket)
        #print('list key:', key)
        try:
            response = s3.list_objects(Bucket=bucket, Prefix=key, Delimiter='/',MaxKeys=1)
            #print(response)
            #return 'Contents' in response
            return True
        except:
            pass
        return False
    else:
      return os_path_isdir(path)

def path_exists(path):
    return isfile(path)

def init_os_wrappers():
    # IMPORTANT NOTE - calling this function remaps certain OS filesystem functions to use our wrapper
    print('initializing s3file wrappers')
    os.path.isfile = isfile
    os.path.isdir = isdir
    os.path.exists = path_exists
    os.listdir = listdir
