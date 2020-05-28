import bitmex
import pandas as pd
import warnings
from swagger_spec_validator.common import SwaggerValidationWarning
import time
from tqdm.auto import trange

warnings.simplefilter("ignore", SwaggerValidationWarning)

if __name__ == "__main__":
    from bitmex import bitmex
    import pandas as pd
    import warnings
    from swagger_spec_validator.common import SwaggerValidationWarning
    import time
    from tqdm.auto import trange

    LOCAL = True
    warnings.simplefilter("ignore", SwaggerValidationWarning)
else:
    LOCAL = False


class DataPuller:

    def __init__(self, save_filename, api_filename, verbose=True):

        self.verbose = verbose
        self.file_name = save_filename
        self.start_index = None
        self.total_pull_size = None
        self.split_size = None
        self.pull_size = None
        self.pull_count = None
        self.split_count = None
        self.api_key = None
        self.api_secret = None
        self.client = None
        self.api_limit = 60
        self.set_start_index()
        self.set_pull_params()
        self.load_client(api_filename)
        self.check_api_limit(True)
        self.df = None

    def set_verbose_status(self, status):
        self.verbose = status

    def set_start_index(self):
        with open(self.file_name) as f:
            last_line = (f.readlines())[-1]
            self.start_index = int(last_line.split(',')[0]) + 1

    def get_last_date(self):
        with open(self.file_name) as f:
            last_line = (f.readlines())[-1]
            return last_line.split(',')[1]

    def set_pull_params(self, total_pull_size=1_000_000, split_size=10_000, pull_size=1_000):
        self.total_pull_size = total_pull_size
        self.split_size = split_size
        self.pull_size = pull_size
        self.pull_count = int(self.split_size / self.pull_size)
        self.split_count = int(self.total_pull_size / self.split_size)

    def set_total_pull_size(self, total_pull_size):
        self.total_pull_size = total_pull_size
        if self.verbose:
            print("Total Pull Size: {}".format(self.total_pull_size))

    def load_client(self, api_key_file, test_status=False):

        keys = open(api_key_file)
        self.api_key = keys.readline()[:-1]
        self.api_secret = keys.readline()
        keys.close()
        self.client = bitmex.bitmex(test=test_status, api_key=self.api_key, api_secret=self.api_secret)

    def check_api_limit(self, verbose=False):
        self.api_limit = self.client.APIKey.APIKey_get().response().metadata.headers['X-RateLimit-Remaining']
        if verbose:
            print("API Pulls Remaining: {}".format(self.api_limit))
            return self.api_limit
        else:
            return self.api_limit

    def single_pull(self, start=0, size=100):
        return self.client.Trade.Trade_get(symbol="XBT", count=size, start=start)

    def pull(self):
        """ Run pull script once parameters are set. """
        for j in trange(0, self.split_count):
            split_start = j * self.split_size
            offset = split_start + self.start_index
            self.df = pd.json_normalize(self.single_pull(start=offset, size=self.pull_size).result()[0])
            self.df.index = range(offset, offset + self.pull_size)
            for i in range(1, self.pull_count):
                starting = (i * self.pull_size) + offset
                if int(self.api_limit) < 10:
                    print("COOLDOWN: {} pulls remaining".format(self.api_limit))
                    time.sleep(15)
                if int(self.api_limit) < 2:
                    print("COOLDOWN: {} pulls remaining".format(self.api_limit))
                    time.sleep(600)
                pulled = self.single_pull(start=starting, size=self.pull_size)
                df_temp = pd.json_normalize(pulled.result()[0])
                self.api_limit = pulled.response().metadata.headers['X-RateLimit-Remaining']
                df_temp.index = range(starting, starting + self.pull_size)
                self.df = self.df.append(df_temp)
                time.sleep(1.08)
            self.df = self.df.to_csv(self.file_name, mode='a', header=False)
            self.df = None
            if self.verbose:
                print("Stored split: {}  -  API Pulls Remaining: {}  -  Last Date: {}".format(j, self.api_limit,
                                                                                              self.get_last_date()))
        self.set_start_index()


if LOCAL:
    KEY_FILE = 'keys.txt'
    FILENAME = "Bitmex_Hist.csv"
    DataPull = DataPuller(FILENAME, KEY_FILE)
    print(DataPull.start_index)
    print(DataPull.get_last_date())
    DataPull.set_total_pull_size(2_000_000)
    # DataPull.pull()
    print(DataPull.start_index)
    print(DataPull.get_last_date())
    print("Complete")
