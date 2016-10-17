import argparse
import cloudshell.api.cloudshell_api
import csv
import time
import os

parser = argparse.ArgumentParser(description='Handle command line arguments')
parser.add_argument('FilePath', help='Path to CSV input file')
parser.add_argument('-u', '--username', help='Quali server username', default='admin')
parser.add_argument('-p', '--password', help='Quali server password', default='admin')
parser.add_argument('-s', '--server', help='Address of Quali server', default='localhost')
parser.add_argument('-d', '--domain', help='Quali domain', default='Global')
parser.add_argument('--port', type=int, help='Quali API port', default=8029)
parser.add_argument('-l', '--LogLocation', help='Directory to place time stamped log file', default='c:\\CloudShell\\Logs')

args = parser.parse_args()

l_time = time.localtime()

t_stamp = str(l_time.tm_year) + str(l_time.tm_mon) + str(l_time.tm_mday) + '_' + str(l_time.tm_hour) + \
          str(l_time.tm_min) + str(l_time.tm_sec)

logLocation = args.LogLocation.rstrip('\\')

logFileName = args.LogLocation + '\\SetConnections_' + t_stamp + '.log'

if not os.path.isdir(logLocation):
    os.makedirs(logLocation)

log = open(logFileName, 'w')

try:
    data = []
    with open(args.FilePath) as input_file:
        datareader = csv.reader(input_file)
        for row in datareader:
            data.append(row)
except IOError as exc:
    print "Unable to read file " + args.FilePath
    exit()

api = None
try:
    api = cloudshell.api.cloudshell_api.CloudShellAPISession(args.server, args.username, args.password, args.domain,
                                                             port=args.port)

except Exception as exc:
    print 'Unable to connect to Quali server'
    print str(exc)
    exit()

for row in data:
    try:
        api.UpdatePhysicalConnection(row[0], row[1])
        print '{0}-{1}: Success'.format(row[0], row[1])
        log.write('{0},{1},Success{2}'.format(row[0], row[1], os.linesep))
    except Exception as exc:
        print '{0}-{1}: {2}'.format(row[0], row[1], exc.message)
        log.write('{0},{1},{2}{3}'.format(row[0], row[1], exc.message, os.linesep))
