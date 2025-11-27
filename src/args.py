import argparse
import json
import sys

def Args():

    parser = argparse.ArgumentParser(prog="unframe", description="Simmple YAML-driven test runner")


    parser.add_argument("-d", "--dir", required=True, help="tests directory")
    parser.add_argument("-e", "--extra-args", default="{}", help='extra args json \'{"account":"proj123","partition":"dev-g"}\')')
    parser.add_argument("-m", "--maxtime", type=int, default=None, help="seconds to wait for all tests to complete")
    parser.add_argument("-n", "--dry-run", action="store_true", default=False, help="only display which tests would be ran")
    parser.add_argument("-o", "--output", default=None, help="output directory path")
    parser.add_argument("-t", "--tag", type=str, default=None,  help="run tests matching this tag")
    parser.add_argument("-v", "--verbose", action="store_true", default=False, help="enable verbose logging")

    args = parser.parse_args()

    try:
        extra_args = json.loads(args.extra_args)
        if not isinstance(extra_args, dict):
            raise ValueError("must be a JSON object")
        args.extra_args = extra_args

    except Exception as e:
        print(f"Invalid --extra-args JSON: {e}")
        sys.exit(2)

    return args
