
MASK_LOGIN = 0
MASK_USER = 1
MASK_ADMIN = 2

USER_TYPE_STR = ['maskuser', 'masklogin', 'maskadmin']

MaskBluStatusMILLABLE = 0
MaskBluStatusFLOPPY = 1
MaskBluStatusMILLED = 2
MaskBluStatusSHIPPED = 3
MaskBluStatusFORGOTTEN = 9

# original code on Solaris had 32-bit time_t that expires 2038-01-19
# we chose maskforever to be a little before then
# this is a time bomb in the DEIMOS and LRIS code
PERPETUAL_DATE = '2035-01-01'

# define what recent means
RECENT_NDAYS = 14

# early in DEIMOS we decided to mill all masks with 0.015 inch tool
# so we hard code the mill tool diameter here
# in the same fashion as Tcl code CGI/makeMill.sin
# in the same fashion as Tcl code Tlib/notifyBadSlits
TOOL_DIAMETER = 15

# admin search results columns
# SEARCH_RESULTS = "d.desid, d.desname, d.desdate, projname, ra_pnt, dec_pnt, " \
#                  "radepnt, o.keckid, o.firstnm, o.lastnm, o.email, o.institution"

