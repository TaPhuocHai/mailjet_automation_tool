# Credit to  https://github.com/regebro/tzlocal
# for dealing with timezone
from tzlocal import get_localzone
import pytz
from datetime import datetime

def convertTimeToUTC(d_input):    
    tz = get_localzone()
    format = "%Y-%m-%d %H:%M"    
    datetime_object = datetime.strptime(d_input, format)
    dt = tz.localize(datetime_object)
    utc = pytz.timezone('UTC')
    t = dt.astimezone(utc)
    
    return t.strftime('%Y-%m-%dT%H:%M:%S')
