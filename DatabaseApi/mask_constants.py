
from collections import defaultdict

MASK_LOGIN = 0
MASK_USER = 1
MASK_ADMIN = 2

USER_TYPE_STR = ['maskuser', 'masklogin', 'maskadmin']

# The mask status is in MaskBlu.status
UNMILLED = 0
FLOPPY = 1
READY = 2
ARCHIVED = 9

# The MaskBlu.status to string
STATUS_STR = defaultdict(
    lambda: 'UNDEFINED',
    {0: 'UNMILLED', 1: 'UNMILLED', 2: 'READY', 9: 'ARCHIVED'}
)

# original code on Solaris had 32-bit time_t that expires 2038-01-19
# we chose maskforever to be a little before then
# this is a time bomb in the DEIMOS and LRIS code
PERPETUAL_DATE = '2035-01-01'

# define what recent number of days
RECENT_NDAYS = 14

# the number of days before a date-use that a mask design must be submitted
OVERDUE = 35

# early in DEIMOS we decided to mill all masks with 0.015 inch tool
# so we hard code the mill tool diameter here
# in the same fashion as Tcl code CGI/makeMill.sin
# in the same fashion as Tcl code Tlib/notifyBadSlits
TOOL_DIAMETER = 15
