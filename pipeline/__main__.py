"""
Pipeline module entrypoint.

Allows running pipeline steps via: python -m pipeline.fetch, etc.
"""

import sys

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] in ["fetch", "run_all"]:
        # Remove the module name from argv
        module = sys.argv.pop(1)

        if module == "fetch":
            from pipeline.fetch import main
            main()
        elif module == "run_all":
            from pipeline.run_all import main
            main()
    else:
        print("Usage: python -m pipeline {fetch|run_all} [options]")
        sys.exit(1)
