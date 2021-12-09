use assert_cmd::Command;
use std::{fs, path::Path};
use tempdir::TempDir;

use crate::{validate_key_hash_pairs, KeyHashPair};

fn test_aws_sync_down(sync_from: &str, sync_to: &str) {
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .arg("s3")
        .arg("sync")
        .arg("--quiet")
        .arg("--acl")
        .arg("bucket-owner-full-control")
        .arg(sync_from)
        .arg(sync_to)
        .assert();

    assert.success();

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
        KeyHashPair("nested/2MiB.bin", "64a2635e42ef61c69d62feebdbf118d4"),
    ];

    validate_key_hash_pairs(sync_to, &key_hash_pairs);
    assert!(fs::remove_dir_all(sync_to).is_ok());
}

#[test]
fn test_sync_trailing_slash() {
    test_aws_sync_down(
        "s3://esthri-test/test_sync_down_default/",
        "tests/data/sync_down/cli_down/",
    );
}

#[test]
fn test_sync_no_trailing_slash() {
    test_aws_sync_down(
        "s3://esthri-test/test_sync_down_default",
        "tests/data/sync_down/cli_down",
    );
}

#[test]
fn test_cp_to_dir() {
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let local_dir = TempDir::new("esthri_cli").unwrap();
    let local_dir_path = local_dir.path().to_str().unwrap();
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .arg("s3")
        .arg("cp")
        .arg("s3://esthri-test/test_sync_down_default/1-one.data")
        .arg(local_dir_path)
        .assert();

    assert.success();

    validate_key_hash_pairs(
        local_dir_path,
        &[KeyHashPair(
            "1-one.data",
            "827aa1b392c93cb25d2348bdc9b907b0",
        )],
    );
}

#[test]
fn test_cp_to_file() {
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let local_dir = TempDir::new("esthri_cli").unwrap();
    let file_path = local_dir.path().join("myfile");

    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .arg("s3")
        .arg("cp")
        .arg("s3://esthri-test/test_sync_down_default/1-one.data")
        .arg(file_path.to_str().unwrap())
        .assert();

    assert.success();

    validate_key_hash_pairs(
        local_dir.path().to_str().unwrap(),
        &[KeyHashPair("myfile", "827aa1b392c93cb25d2348bdc9b907b0")],
    );
}

#[test]
fn test_aws_fallthrough() {
    let mut cmd = Command::cargo_bin("esthri").unwrap();

    // Esthri should fallback to the AWS executable if invoked in compat mode
    // and if it doesn't know how to handle the arg
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .env("ESTHRI_AWS_PATH", "echo")
        .arg("unknown_command")
        .assert();

    assert.success().stdout("unknown_command\n");
}

#[test]
fn test_aws_fallthrough_cp_option() {
    let mut cmd = Command::cargo_bin("esthri").unwrap();

    // Try with some options we know but one we don't (ls)
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .env("ESTHRI_AWS_PATH", "echo")
        .arg("s3")
        .arg("ls")
        .assert();

    assert.success().stdout("s3 ls\n");
}

#[cfg(feature = "compression")]
#[test]
fn test_cp_falls_back_with_compression() {
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let local_dir = TempDir::new("esthri_cli").unwrap();
    let local_dir_path = local_dir.path().to_str().unwrap();
    // this file doesn't exist on S3, but
    // s3://esthri-test/test_download/27-185232-msg.csv.gz does. In
    // compression mode, esthri should fall back to downloading (and
    // decompressing) the compressed version transparently.
    let key = "s3://esthri-test/test_download/27-185232-msg.csv";
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .env("ESTHRI_AWS_COMPAT_MODE_COMPRESSION", "1")
        .arg("s3")
        .arg("cp")
        .arg(key)
        .arg(local_dir_path)
        .assert();

    assert.success();

    validate_key_hash_pairs(
        local_dir_path,
        &[KeyHashPair(
            "27-185232-msg.csv",
            "e8b32017e42f2e727316d68ea72c2832",
        )],
    );
}

#[test]
fn test_aws_sync_down_filter() {
    let sync_from = "s3://esthri-test/test_sync_down_default";
    let local_dir = TempDir::new("esthri_cli").unwrap();
    let sync_to = local_dir.path().to_str().unwrap();
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .arg("s3")
        .arg("sync")
        .arg(sync_from)
        .arg(sync_to)
        .arg("--exclude")
        .arg("*")
        .arg("--include")
        .arg("*.bin")
        .assert();

    assert.success();

    // Shouldn't have downloaded files that don't match the filtering
    assert!(!Path::new(sync_to).join("1-one.data").exists());
    assert!(!Path::new(sync_to).join("3-three.junk").exists());

    let key_hash_pairs = [
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("nested/2MiB.bin", "64a2635e42ef61c69d62feebdbf118d4"),
    ];

    validate_key_hash_pairs(sync_to, &key_hash_pairs);
    assert!(fs::remove_dir_all(sync_to).is_ok());
}

#[test]
fn test_aws_sync_down_filter_exclude_only() {
    let sync_from = "s3://esthri-test/test_sync_down_default";
    let local_dir = TempDir::new("esthri_cli").unwrap();
    let sync_to = local_dir.path().to_str().unwrap();
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .arg("s3")
        .arg("sync")
        .arg(sync_from)
        .arg(sync_to)
        .arg("--exclude")
        .arg("*.bin")
        .assert();

    assert.success();

    // Shouldn't have downloaded files that don't match the filtering
    assert!(!Path::new(sync_to).join("2-two.bin").exists());
    assert!(!Path::new(sync_to).join("nested/2MiB.bin").exists());

    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
    ];

    validate_key_hash_pairs(sync_to, &key_hash_pairs);
    assert!(fs::remove_dir_all(sync_to).is_ok());
}

#[test]
fn test_aws_sync_down_filter_include_only() {
    let sync_from = "s3://esthri-test/test_sync_down_default";
    let local_dir = TempDir::new("esthri_cli").unwrap();
    let sync_to = local_dir.path().to_str().unwrap();
    let mut cmd = Command::cargo_bin("esthri").unwrap();
    let assert = cmd
        .env("ESTHRI_AWS_COMPAT_MODE", "1")
        .arg("s3")
        .arg("sync")
        .arg(sync_from)
        .arg(sync_to)
        .arg("--include")
        .arg("*.bin")
        .assert();

    assert.success();

    // This should download everything according to the AWS docs,
    // because there's no explicit exclude
    let key_hash_pairs = [
        KeyHashPair("1-one.data", "827aa1b392c93cb25d2348bdc9b907b0"),
        KeyHashPair("2-two.bin", "35500e07a35b413fc5f434397a4c6bfa"),
        KeyHashPair("3-three.junk", "388f9763d78cecece332459baecb4b85"),
        KeyHashPair("nested/2MiB.bin", "64a2635e42ef61c69d62feebdbf118d4"),
    ];

    validate_key_hash_pairs(sync_to, &key_hash_pairs);
    assert!(fs::remove_dir_all(sync_to).is_ok());
}
