# Esthri (S3)

[![Build Status](https://jenkins.ci.swift-nav.com/buildStatus/icon?job=swift-nav%2Festhri%2Fmaster)](https://jenkins.ci.swift-nav.com/job/swift-nav/job/esthri/job/master/)

Extremely simple (memory stable) S3 client that supports get, put, head, list,
and sync.

```
esthri 0.1.0
Simple S3 file transfer utility.

USAGE:
    esthri <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    abort           Manually abort a multipart upload
    get             Download an object from S3
    head-object     Retreive the ETag for a remote object
    help            Prints this message or the help of the given subcommand(s)
    list-objects    List remote objects in S3
    put             Upload an object to S3
    s3-etag         Compute and print the S3 ETag of the file
    sync            Sync a directory with S3
```
