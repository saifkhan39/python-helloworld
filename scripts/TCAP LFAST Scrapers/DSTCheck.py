import datetime
from datetime import timedelta
import arrow
import pytz

def dstCheck(date):
    utc = arrow.get(date)
    local = utc.to('US/Central')
    local_string = str(local)
    if local_string[-4] == '5':
        print('It is DST')
        return('DST')
    else:
        print('It is not DST')
        return('notDST')
        
def hours(date):
    today = dstCheck(date)
    noon = dstCheck(date + datetime.timedelta(hours = 12))
    if today == noon:
        return(24)
    else:
        if noon == 'DST':
            return(23)
        else:
            return(25)

