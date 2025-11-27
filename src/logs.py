import sys

class Logs:

    def __init__(self, args):
        self.verbose = args.verbose

    def perf(self, message):
        print(message)

    def debug(self, message):
        if self.verbose:
            print("DEBUG:", message)

    def info(self, message):
        print("INFO:", message)

    def warn(self, message):
        print("WARN:", message)

    def error(self, message):
        print("ERROR:", message)

    def fatal(self, message):
        print(f"FATAL: {message}")
        sys.exit(1)
