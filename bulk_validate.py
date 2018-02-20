import getopt
import itertools
from datetime import datetime
from bagit import *
import platform
from tqdm import tqdm

paths_file = ""

d = date.today()
td = d.isoformat()

report_logger = logging.getLogger(__name__)
trim = []


class BagValidator:
    def __init__(self, bulk_path: list=None, sing_path: str=None) -> None:
        self.bulk_path = bulk_path
        self.single_path = sing_path
        self.validator_log = LogName()

    def bag_validate_bulk(self):
        for p in self.bulk_path:
            self._validator(p.strip("\n"))

    def bag_validate_single(self, p=None):
        if p is not None:
            self._validator(p.strip("\n"))
            return
        self._validator(self.single_path)

    def _validator(self, pth):
        try:
            bag = Bag(pth)
            report_logger.info("VALIDATING: \t{}".format(pth))
            bag.validate(processes=8)
            report_logger.info("VALID: \t{}".format(pth))
            return 1
        except BagValidationError as e:
            for d in e.details:
                if isinstance(d, ChecksumMismatch):
                    report_logger.error('Expected {} to have {} checksum of {} but found {}'
                             .format(d.path, d.algorithm, d.expected, d.found))
                elif isinstance(d, FileMissing):
                    report_logger.error('The expected file {} is missing'.format(d.path))
                elif isinstance(d, UnexpectedFile):
                    report_logger.error('Found a file {} that is not in the manifest'.format(d.path))
                else:
                    pass
            report_logger.error('{}: {}'.format(e, pth))
            report_logger.info("INVALID: \t{}".format(pth))
            return 1
        except OSError as we:
            report_logger.critical(we.strerror)
            report_logger.critical(we.filename)
            report_logger.info("INVALID: \t{}".format(pth))
            return 1
        except Exception as e:
            report_logger.critical(e)
            report_logger.info("INVALID: \t{}".format(pth))
            return 1


class BagFinder:
    def __init__(self, p):
        self.search_path = os.path.abspath(p)
        self.bags = []

    def find_bag_path(self):
        report_logger.info("Scanning {} for bags.".format(self.search_path))
        spinner = itertools.cycle(['-', '/', '|', '\\'])
        for root, dirs, files in os.walk(self.search_path):
            # Remove hidden directories from the search tree.
            dirs[:] = [d for d in dirs if not self._is_file_hidden(os.path.join(root, d))]
            if self._is_file_hidden(root):
                continue
            for f in files:
                sys.stdout.write(next(spinner))
                sys.stdout.flush()
                sys.stdout.write('\b')
                if f == "bagit.txt":
                    report_logger.info("Found a bag {}".format(root))
                    # Trim the branch
                    dirs[:] = [d for d in dirs if not root]
                    self.bags.append(root)
                    break

    @staticmethod
    def _is_file_hidden(file):
        try:
            if platform.system() == 'Windows':
                if os.stat(file).st_file_attributes == 18:
                    return True
            else:
                if file == '.':
                    return True
            return False
        except Exception as e:
            print(e)


class LogName:
    def __init__(self, assigned=None, location=os.getcwd()):
        self.assigned = assigned
        self.location = location

    def get_log_name(self):
        if not self.assigned:
            time = datetime.now()
            return '{}{}{}_validation_report.log'.format(time.year, str(time.month).zfill(2), str(time.day).zfill(2))


def set_loggers(cwd):
        report_logger.setLevel(logging.DEBUG)

        handler_rl = logging.FileHandler(join(cwd, LogName().get_log_name()))
        command_line = logging.StreamHandler(sys.stdout)

        formatter = logging.Formatter('%(levelname)s: %(asctime)s %(message)s')

        handler_rl.setFormatter(formatter)
        command_line.setFormatter(formatter)

        report_logger.addHandler(handler_rl)
        report_logger.addHandler(command_line)


def get_time_in_seconds():
    time = datetime.now()
    return time.hour*3600 + time.minute*60 + time.second


def help_text():
    print("Usage: bulk_validate.py [-f <PATH>] OR [-w <PATH>]\n")
    print("Where -f <PATH> is the path to a plain text file. Each line of the file should be a unique path to \n")
    print("a bag you want to validate.\n")
    print("Where -w <PATH> is the path to a TLD that may have bags underneath. This will walk the entire tree "
          "looking for bags and will validate any bag it finds.")


if __name__ == '__main__':
    try:
        opts, args = getopt.getopt(sys.argv[1:], "hlp:f:i:w:s:")
    except getopt.GetoptError:
        help_text()
        sys.exit(2)

    set_loggers(os.getcwd())

    for opt, arg in opts:
        if opt == "-f":
            paths_file = open(arg, "r")
            for path in paths_file.readlines():
                bval = BagValidator(None)
                bval.bag_validate_single(path)
        if opt == "-w":
            bfinder = BagFinder(arg)
            bfinder.find_bag_path()
            bval = BagValidator(bfinder.bags)
            bval.bag_validate_bulk()
        if opt == "-s":
            # This is the path to the top of a single bag.
            bval = BagValidator()
            bval.bag_validate_single(arg)
        if opt == "-h":
            help_text()