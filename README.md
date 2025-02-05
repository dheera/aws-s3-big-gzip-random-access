# AWS S3 gzip random access

## Problem

Sometimes datasets are on S3 in some gzipped format, e.g. .csv.gz, with ungodly file sizes.

Polygon, for example, has option data stored in 100GB+ .csv.gz files on S3.

You want to random access parts of them for some ML training, analysis, genomics, whatever.

## Best solution

The best architectural solution is NOT to use this repo.
If you have control over the dataset, the best solution is to NOT use vanilla gzip and instead use bgzip, bzip2, or some other random-access-friendly compression algorithm.
However, that is not always possible, because someone else made the dataset, they used vanilla gzip, there are petabytes of it somewhere on S3, and you the poor engineer still needs to random access them.
Enter this repo.

## This solution

Normally, you cannot random access vanilla gzip files. Even if you search for valid DEFLATE block boundaries, the stream of blocks may refer to prior blocks.

This uses indexed\_gzip to build an index to the file so that it is random-accessible with the index.
Basically you need to read the full file only once, store the index, and then you can random access it later.

The index-building can be done on AWS so that you aren't wasting your downlink on it. You will get the best performance if you do it from an EC2 instance in the same region as the petabyte dataset.

You can then take that index wherever you want, and then random access pieces of the massive gzipped files from there without downolading the whole file.

## Future of this repo

Currently, I don't imagine many people need this. It serves my needs in the current form. If demand is high I'll make a proper Python package out of it, or please do it for me and submit a pull request if you'd like.
