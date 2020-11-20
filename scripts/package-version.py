#!/usr/bin/env python3

import sys
import toml

CARGO_TOMLS = [
    "src/esthri/Cargo.toml",
    "src/esthri_rusoto/rusoto_nativetls/Cargo.toml",
    "src/esthri_rusoto/rusoto_rustls/Cargo.toml",
]


def update_toml(cargo_toml_path, version):
    major, minor, bugfix = version
    cargo_toml = toml.load(cargo_toml_path)
    cargo_toml["package"]["version"] = "{}.{}.{}".format(major, minor, bugfix)
    with open(cargo_toml_path, "w") as toml_out:
        toml.dump(cargo_toml, toml_out)

def get_current_version(cargo_toml_path):
    cargo_toml = toml.load(cargo_toml_path)
    version = cargo_toml["package"]["version"]
    major, minor, bugfix = map(int, version.split("."))
    return major, minor, bugfix


def main():
    major, minor, bugfix = get_current_version(CARGO_TOMLS[0])

    if len(sys.argv) < 2:
        sys.stderr.write("ERROR: not enough arguments")
        sys.exit(1)

    if sys.argv[1] == "bump-major":
        major += 1
        minor, bugfix = 0, 0
    elif sys.argv[1] == "bump-minor":
        minor += 1
        bugfix = 0
    elif sys.argv[1] == "bump-bugfix":
        bugfix += 1
    elif sys.argv[1] == "get-current":
        print(f"{major}.{minor}.{bugfix}")
        sys.exit(0)
    else:
        sys.stderr.write("ERROR: unknown command")
        sys.exit(1)

    for cargo_toml_path in CARGO_TOMLS:
        update_toml(cargo_toml_path, (major, minor, bugfix))


if __name__ == '__main__':
    main()
