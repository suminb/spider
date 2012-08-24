from database import Database

class Frontend:
    def __init__(self, opts):
        self.opts = opts

        # shared across multiple threads
        self.shared = {}

    def run(self):
        raise Exception("Not implemented")

    def prepare_curses(self):
        pass

    def cleanup_curses(self):
        pass


class SingleMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        report = fetch_url(self.opts["url"])

        if self.opts["generate_report"]:
            generate_report(report)

class MultiThreadingMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        pass


class CreateDBMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        with Database(self.opts["db_path"]) as db:
            with open("scheme.txt") as f:
                # FIXME: This may break in some cases
                for sql in f.read().split(";"):
                    db.execute(sql)


class ReportMode(Frontend):
    def __int__(self, opts):
        super(Frontend, self).__init__(opts)

    def run(self):
        if "db_path" not in self.opts:
            raise Exception("Database path is not specified.")
        else:
            ReportMode.generate_report(self.opts["db_path"])

    @staticmethod
    def generate_report(db_path, session_report=None):
        """Prints out a status report to standard output. This function may be called from outside this class."""
        
        from database import Database

        with Database(db_path) as db:
            url_count = db.url_count
            fetched_url_count = db.fetched_url_count

            if session_report != None:
                print "-[ Spider Report: This session ]------------------------------------"
                print "  Number of fetch requests sent out: %d" % (opts["n_urls"])
                print "  Number of successful fetches: %s" % session_report['succeeded']
                print "  Live proxy hit ratio: %.02f%%" % (100.0 * session_report['succeeded'] / opts["n_urls"])
                print "  Sum of size of fetched documents: %d" % session_report['fetched_size']
                print "  Number of newly found URLs: %d" % session_report['new_urls_count']
                print

            print "-[ Spider Report: Overall summary ]------------------------------------"
            print "  Total number of URLs: %d" % url_count
            print "  Number of fetched URLs: %d" % fetched_url_count
            print "  Progress: %.02f%%" % (100.0 * fetched_url_count / url_count)


import sys
import getopt

def parse_args(args):
    optlist, args = getopt.getopt(args, "u:n:t:d:sg", ("create-db", "single=", "generate-report"))
    
    # default values
    opts = {
        "run_mode": "multithreading",
    }

    for o, a in optlist:
        if o == '-n':
            opts["n_urls"] = int(a)

        elif o == '-t':
            opts["n_proc"] = int(a)

        elif o == '-d':
            opts["db_path"] = a

        elif o in ("-u", "--url"):
            opts["url"] = a

        elif o == "--create-db":
            opts["run_mode"] = "create_db"

        elif o in ("-s", "--single"):
            opts["run_mode"] = "single"

        elif o in ("-m", "--multithreading"):
            opts["run_mode"] = "multithreading"

        elif o in ("-g", "--generate-report"):
            opts["run_mode"] = "generate_report"

    return opts

def main():
    opts = parse_args(sys.argv[1:])

    run_mode = opts["run_mode"]

    if run_mode == "single":
        pass

    elif run_mode == "create_db":
        fend = CreateDBMode(opts)
        fend.run()

    elif run_mode == "generate_report":
        fend = ReportMode(opts)
        fend.run()

if __name__ == "__main__":
    main()
