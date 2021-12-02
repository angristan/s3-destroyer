# s3-destroyer

Iteratively calls `ListObjects`, add objects keys to a buffer and calls `DeleteObject` in goroutines.

## Usage

```
  -access-key string
        s3 access-key
  -bucket string
        s3 bucket
  -buffer int
        Size of the buffer in number of objects (default 32768)
  -endpoint string
        s3 endpoint
  -region string
        s3 region
  -secret-key string
        s3 secret-key
  -workers int
        Number of workers to use (default 512)
```

## TODO

- [] Support for whole account wipe
- [] Handle rate limiting
