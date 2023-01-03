# Esthri (S3)

[![Build Status](https://jenkins.ci.swift-nav.com/buildStatus/icon?job=swift-nav%2Festhri%2Fmaster)](https://jenkins.ci.swift-nav.com/job/swift-nav/job/esthri/job/master/)

Extremely simple (memory stable) S3 client that supports get, put, head, list,
and sync.

```
esthri 6.3.0
Simple S3 file transfer utility.

USAGE:
    esthri <SUBCOMMAND>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

SUBCOMMANDS:
    abort           Manually abort a multipart upload
    etag            Compute and print the S3 ETag of the file
    get             Download an object from S3
    head-object     Retreive the ETag for a remote object
    help            Prints this message or the help of the given subcommand(s)
    list-objects    List remote objects in S3
    put             Upload an object to S3
    serve           Launch an HTTP server attached to the specified bucket
    sync            Sync a directory with S3
```

## AWS S3 Compatibility Layer

The esthri CLI tool additionally provides a AWS CLI compatibility mode where it
is able to handle `cp` and `sync` operations with an identical CLI interface as
the `aws s3` tool. Currently, only the basic case is implemented for these
commands and only some optional arguments are handled.

To use, the esthri binary must either be:

- Named or hard-linked as `aws`
- Run with the `ESTHRI_AWS_COMPAT_MODE` environment variable set

In this mode, esthri will attempt to transparently invoke the real `aws` tool if
it encounters a command it cannot handle. For this to work, the path to the real
`aws` tool should be put in an environment variable named `ESTHRI_AWS_PATH`. For
example: `ETHRI_AWS_PATH=/usr/bin/aws`. If this environment variable is not
specified then esthri will default to invoking `aws.real` as the `aws` tool.

```
$ ln -s /usr/local/bin/aws /usr/local/bin/aws.real
$ ESTHRI_AWS_COMPAT_MODE=1 esthri s3 help # prints the S3 help text, as generated by the aws command
```

### Transparent Sync Compression
esthri can transparently compress files such that they are compressed within S3
but not on the local filesystem. To enable, either set the
`ESTHRI_AWS_COMPAT_MODE_COMPRESSION` environment variable or use the
`--transparent-compression` CLI option. For example:

```
ESTHRI_AWS_COMPAT_MODE=1 esthri s3 sync mydirectory/ s3://esthri-test/myfiles/ --transparent-compression # syncs as normal, however files in S3 are gzipped
ESTHRI_AWS_COMPAT_MODE=1 esthri s3 sync s3://esthri-test/myfiles/ mynewdirectory/ --transparent-compression # syncs and decompresses files as they are synced down. mydirectory and mynewdirectory will now contain the same files
```

esthri makes use of S3 metadata to do this transparent compression, storing the
esthri version number in the metadata of files it has compressed. For this
reason, the decompression will only work for files that esthri has compressed
itself.

## AWS credential provider selection

It is highly recommended to set a specific credential provider which gives you
better security and granular level of control. e.g:

```
CREDENTIAL_PROVIDER=profile esthri s3 sync s3://esthri-test/myfiles/ mynewdirectory/
```

`ESTHRI_CREDENTIAL_PROVIDER=env` ---> fetched from environment variables
`ESTHRI_CREDENTIAL_PROVIDER=profile` ---> fetched from default credential file
`ESTHRI_CREDENTIAL_PROVIDER=container` ---> fetched from task's IAM role in ECS
`ESTHRI_CREDENTIAL_PROVIDER=instance_metadata` ---> fetched from instance metadata service
`ESTHRI_CREDENTIAL_PROVIDER=k8s` ---> fetched from kubernetes auth service
`ESTHRI_CREDENTIAL_PROVIDER=` ---> explicitly using default credential provider if empty

If not set the program will fall back to default credential provider in which the
above providers are iterated through in the above order until the first working value is found

## Releasing

[cargo-release](https://github.com/crate-ci/cargo-release) is used to automate
releases of esthri using the [release.toml](./release.toml) file for release
configuration.

To perform a release, run `cargo release <release-level> -x`.

## Copyright

```
Copyright (C) 2021 Swift Navigation Inc.
Contact: Swift Navigation <dev@swiftnav.com>

This source is subject to the license found in the file 'LICENSE' which must be
be distributed together with this source. All other rights reserved.

THIS CODE AND INFORMATION IS PROVIDED "AS IS" WITHOUT WARRANTY OF ANY KIND,
EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE IMPLIED
WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A PARTICULAR PURPOSE.
```
