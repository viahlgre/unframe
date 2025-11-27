#!/usr/bin/python3

from args import Args
from logs import Logs
from tests import TestSet

def main():

    args = Args()
    log = Logs(args)
    tests = TestSet(args, log)

    tests.run()

    log.info(args)
    log.info(tests)


if __name__ == "__main__":
    main()
