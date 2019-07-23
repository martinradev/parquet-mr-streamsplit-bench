This is a temporary test project to gather stats for the new ```BYTE_STREAM_SPLIT``` encoding being reviewed for Apache Parquet.

I used my local changes to parquet-mr to gather numbers on the reduction in size when the new encoding is used for FP column data.

The comparison covers both 32-bit and 64-bit floating point scientific data. The datasets can be downloaded in compressed form from the two following links:
* https://userweb.cs.txstate.edu/~burtscher/research/datasets/FPsingle/
* https://userweb.cs.txstate.edu/~burtscher/research/datasets/FPdouble/
If you happen to attempt to reproduce the results, please read the instructions on decompressing the data before using it.

The comparison is based on Apache Avro which stores the data using Apache Parquet. It is based on my extension of the parquet-mr project.
The Avro schema used for comparison is the following:
```
{
    "type": "record",
    "name": "Test",
    "fields": [{"name": "value", "type": "double"}]
}
```
For F32 data "double" was replaced with "float".

I examined four different options for encoding and/or compressing the data:
* no dictionary, no compression
* no dictionary, compression using gzip
* with dictionary, compression using gzip
* the new byte stream split encoding, compression using gzip

For the test I used gzip instead of zstd or brotli because the usage of zstd and brotli adds more dependencies I did not have installed.
In my original report I used zstd and similar improvements were observed as with gzip.

Using dictionary encoding did not improve performance for anything but one of the entries. For that reason it is not included in the evaluation.
The time to compress and store data to disk is dominated by the time necessary to store to disk. For many cases, the new encoding led to slightly better performance likely due to less data needed to be stored to disk.

Results:

| F32 dataset name | no compression (MB) | gzip (MB) | byte_stream_split + gzip (MB) | % Improvement with new encoding |
|------------------|---------------------|-----------|-------------------------------|---------------------------------|
| msg_bt           | 133.25              | 112.54    | 88.34                         | +18.16                          |
| msg_lu           | 97.1                | 88.63     | 73.61                         | +15.48                          |
| msg_sp           | 145.12              | 121.76    | 91.02                         | +21.18                          |
| msg_sweep3d      | 62.89               | 54.29     | 41.81                         | +19.84                          |
| num_brain        | 70.95               | 62.83     | 53.34                         | +13.37                          |
| num_comet        | 53.69               | 46.69     | 40.18                         | +12.12                          |
| num_control      | 79.79               | 73.72     | 67.52                         | +7.77                           |
| num_plasma       | 17.55               | 10.79     | 11.80                         | -5.75                           |
| obs_error        | 31.09               | 21.52     | 21.66                         | -0.45                           |
| obs_info         | 9.47                | 8.36      | 6.66                          | +17.95                          |
| obs_spitzer      | 99.13               | 84.06     | 79.10                         | +5.00                           |
| obs_temp         | 19.97               | 18.49     | 17.03                         | +7.31                           |

| F64 dataset name | no compression (MB) | gzip (MB) | byte_stream_split + gzip (MB) | % Improvement with new encoding |
|------------------|---------------------|-----------|-------------------------------|---------------------------------|
| msg_bt           | 266.45              | 236.20    | 199.63                        | +13.72                          |
| msg_sppm         | 279.06              | 38.61     | 37.39                         | +0.43                           |
| msg_sweep3d      | 125.76              | 115.18    | 96.55                         | +14.81                          |
| num_brain        | 141.87              | 133.36    | 115.77                        | +12.39                          |
| num_comet        | 107.37              | 92.80     | 79.81                         | +12.09                          |
| num_control      | 159.54              | 150.97    | 139.93                        | +6.91                           |
| num_plasma       | 35.09               | 19.81     | 26.11                         | -17.95                          |
| obs_error        | 62.17               | 43.08     | 47.48                         | -7.07                           |
| obs_info         | 18.93               | 16.58     | 14.49                         | +11.04                          |
| obs_spitzer      | 198.22              | 162.60    | 157.33                        | +2.65                           |
| obs_temp         | 39.94               | 38.57     | 35.58                         | +7.48                           |

