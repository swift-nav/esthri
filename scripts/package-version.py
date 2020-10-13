#!/usr/bin/env python3

import sys
import toml

cargo_toml = toml.load("Cargo.toml")
version = cargo_toml["package"]["version"]

major, minor, bugfix = map(int, version.split("."))

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

cargo_toml["package"]["version"] = "{}.{}.{}".format(major, minor, bugfix)

with open("Cargo.toml", "w") as toml_out:
    toml.dump(cargo_toml, toml_out)
