# ds
A high-performance c++-based large/small dataset loader.
## Usage

```python
from ds import Dataset
urls = [
    '....00000.tar',
    '....00001.tar',
    '....00002.tar',
]
shuffle_buffer_size = 256_000
num_workers = 128
num_blocks = 2
num_examples = 1024
wakeup = 1024*1024*10

headers = ['Authorization: Bearer ...']
ds = Dataset(
    urls,
    headers,
    shuffle_buffer_size,
    num_workers,
    num_blocks,
    num_examples,
    wakeup,
)
ds.Start()
inputs = ds.Next()
```

# To-Do List

- [ ] Stopping
- [ ] Bucket Support for Sharding
- [ ] Online/Offline Caching
- [ ] S3 Support
- [ ] GCS Support
