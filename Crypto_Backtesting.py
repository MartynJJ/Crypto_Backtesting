from bitmex import bitmex
import json
from datetime import datetime as dt
import pandas as pd
import warnings
from swagger_spec_validator.common import SwaggerValidationWarning




if __name__ == "__main__":
    print("Hello_World")
    REMOTE = False
    warnings.simplefilter("ignore", SwaggerValidationWarning)



client = bitmex(test=True)
# test = client.Instrument(symbol="XBT", count=10)
# test = pd.DataFrame(client.Trade.Trade_get(symbol="XBT", count=2, columns=["price"]).result()[0])
test = client.Trade.Trade_get(symbol="XBT", count=200).result()[0]

# print(dir(test))
