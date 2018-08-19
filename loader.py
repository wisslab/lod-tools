from SPARQLWrapper import SPARQLWrapper, JSON
import time
import argparse
import sys

parser = argparse.ArgumentParser(description="Load Turtle data via SPARQL")
parser.add_argument("-e", "--endpoint", metavar="ENDPOINT", help="The UPDATE endpoint", required=True, dest="endpoint")
parser.add_argument("-g", "--graph", metavar="GRAPH", default="default", help="The named GRAPH, default is default graph.", dest="graph")
parser.add_argument("-f", "--file", metavar="FILE", help="The turtle file to load.", required=True, dest="file")
args = parser.parse_args()

print("Endpoint: {}".format(args.endpoint))
print("Graph: {}".format(args.graph))
print("File: {}".format(args.file))

proceed = input("\nShould I proceed? (y/N)")
if proceed != "y":
    sys.exit()

sparql = SPARQLWrapper(args.endpoint)


def insert(prefixes, graph, data):
    query = "%s INSERT { GRAPH <%s> { %s } } WHERE {}" % (prefixes, graph, data)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    sparql.method = 'POST'
    return sparql.query()


def log(message):
    f = open("log.txt", mode="a", encoding="utf8")
    f.write(str(message))


class Reader:
    linecount = 0
    _file = None

    def __init__(self, file):
        self._file = file

    def readline(self):
        self.linecount += 1
        return self._file.readline()

    def skiplines(self, number):
        for i in range(number):
            line = self.readline()
            if not line:
                break

    def skipafter(self, linenumber):
        # assert linenumber>=self.linecount
        while(linenumber > self.linecount):
            self.readline()

    def readlines(self, number):
        lines = []
        for i in range(number):
            line = self.readline()
            if line:
                lines.append(line)
            else:
                break
        if len(lines) > 0:
            return "".join(lines)
        else:
            return None

    def readUntilBlank(self, number=1):
        lines = []
        done = 0
        while True:
            line = self.readline()
            if line:
                lines.append(line)
            else:
                break
            if line == "\n":
                done += 1
                if done == number:
                    break
        if len(lines) > 0:
            return ("".join(lines), done)
        else:
            return (None, done)


with open(args.file, encoding="utf8") as f:
    r = Reader(f)
    prefixes = ""
    #  prefixes, chunks = r.readUntilBlank()
    #  prefixes = prefixes.replace("@prefix", "PREFIX")
    #  prefixes = prefixes.replace(" .", "")
    #  print("Prefixes used:")
    #  print(prefixes)

    skipped = 4594418
    r.skipafter(skipped)

    resources = 0
    finished = False
    start = time.perf_counter()
    while True:
        cycle = time.perf_counter()
        last = r.linecount
        data, chunks = r.readUntilBlank(3000)
        resources += chunks
        if not data:
            break
        try:
            insert(prefixes, args.graph, data)
        except Exception as e:
            print("ERROR. More info in log.txt.")
            log("Error occured at line %d:" % r.linecount)
            log(e)
            log("Data: " + data)
        print("%d: %d lines (%d) -- Time: %.2f seconds (%.2f lines/s), total: %.2f seconds (%.2f lines/s)" %
              (resources,
               r.linecount - last,
               r.linecount,
               time.perf_counter() - cycle,
               (r.linecount - last) / (time.perf_counter() - cycle),
               time.perf_counter() - start,
               (r.linecount - skipped) / (time.perf_counter() - start)))

    print("Finished!")
