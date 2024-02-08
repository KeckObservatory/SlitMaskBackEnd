#! /usr/bin/python3

################################################

import sys
import datetime

from general_utils import commitOrRollback
import logger_utils as log_fun

from general_utils import do_query
from mask_constants import MASK_ADMIN


################################################


def maskStatus(db, bluid, newstatus):
    """
    update the status of a blueprint

    inputs:
    db        database object which is already connected with suitable privs
    bluid     primary key in MaskBlu
    newstatus one of the above MaskBluStatus values

    outputs:
    none

    side effects:
    MaskBlu with bluid gets
      status          = newstatus
      millseq         = null string
    """
    log = log_fun.get_log()
    maskStatusUpdate = "update MaskBlu set status = %s where bluid = %s"

    try:
        db.cursor.execute(maskStatusUpdate, (newstatus, bluid))
    except Exception as e:
        log.error(f"maskStatuUpdate failed: {db.cursor.query}: "
                  f"exception class {e.__class__.__name__}: {e}")

        # need to return error information along with FAILURE
        return False
    # end try

    # develop debugging
    log.info(f"updated bluid {bluid} new status {newstatus}")

    # foo need to fix database create because null millseq is normal for new masks
    newmillseq  = '  '

    maskMillseqUpdate = "update MaskBlu set millseq = %s where bluid = %s"

    try:
        db.cursor.execute(maskMillseqUpdate, (newmillseq, bluid))
    except Exception as e:
        log.error(f"maskMillseqUpdate failed bluid {bluid}: {db.cursor.query}: "
                  f"exception class {e.__class__.__name__}: {e}")

        # need to return error information along with FAILURE
        return False
    # end try

    # develop debugging
    log.info(f"bluid {bluid} new millseq {newmillseq}")

    errcnt, message = commitOrRollback(db)

    if errcnt != 0:
        print("commitOrRollback failed: %s" % (message))
        return False
    # end if

    print("commitOrRollback worked, db should be changed")
    return True

# end def maskStatus()

################################################


def my_design(user_info, db, desid):
    """
    Is the logged in user the Design Author or the Blueprint Observer?
    This can decide whether a non-admin user may modify mask records.

    Note that because this uses DesId it is possible that more than
    one Blueprint was derived from this Design, and those Blueprints
    might have different values of BluPId for different Observers.
    Therefore this code is allowing a logged in user who is a
    Blueprint Observer for one Blueprint with that DesId to modify
    all other Blueprints with that DesId even if those other
    Blueprints have different Blueprint Observers.

    In actual practice the mask design tools never allowed for a
    single Mask Design to be re-used to make different Mask Blueprints
    that could be suitable for observing the same field at different
    hour angles.  In actual practice the mask design tools create an
    entirely separate Design for each Blueprint even if every slitlet
    in the Design is the same.

    There do exists some database records where one Design DesId is
    found with more than one Blueprint, but almost all of those are
    "permanent" "calibration" masks whose Author/Observer values
    are mask admin users anyway.

    """
    if user_info.user_type == MASK_ADMIN:
        return True

    curse = db.cursor()
    params = (desid, user_info.ob_id, desid, user_info.ob_id)
    if not do_query('design_person', curse, params):
        return False

    results = curse.fetchall()

    len_results = len(results)

    if len_results == 0:
        # No, this is not my Design
        return False

    # Yes, this is my Design
    return True

################################################


def my_blueprint(user_info, db, bluid):

    if user_info.user_type == MASK_ADMIN:
        return True

    curse = db.cursor()
    params = (bluid, user_info.ob_id, bluid, user_info.ob_id)
    if not do_query('blue_person', curse, params):
        return False

    results = curse.fetchall()

    len_results = len(results)

    if len_results == 0:
        return False

    return True


################################################
# TODO currently not used
# def isThisMyMask( db, maskid ):
#
#     # Is the logged in user the Blueprint Observer or the Design Author?
#     # This can decide whether a non-admin user may modify mask records.
#     log = log_fun.get_log()
#     maskHumanQuery      = (
#     "select email from Observers"
#     " where ObId in ("
#     " (select BluPId from MaskBlu    where BluId = (select BluId from Mask where MaskId = %s)),"
#     " (select DesPId from MaskDesign where DesId = (select DesId from MaskBlu where BluId = (select BluId from Mask where MaskId = %s)))"
#     " ) and email = %s;"
#     )
#
#     # during development display the query
#     print(maskHumanQuery % (maskid, maskid, db.get_user_email()))
#
#     try:
#         db.cursor.execute(maskHumanQuery, (maskid, maskid, db.maskumail) )
#     except Exception as e:
#         log.error(
#         "%s failed: %s: exception class %s: %s"
#         % ('maskHumanQuery', db.cursor.query, e.__class__.__name__, e) )
#
#         return False
#     # end try
#
#     results = db.cursor.fetchall()
#
#     len_results  = len(results)
#
#     if len_results == 0:
#         # No, this is not my Mask
#         return False
#     else:
#         # Yes, this is my Mask
#         return True
#
# # end def isThisMyMask()

################################################


# TODO no need,  kept as filler
# def dumpselect(db):
#     """
#     for introspection during development
#     """
#     log = log_fun.get_log()
#     # inputs:
#     # db        database object which has successfully executed a select
#
#     # outputs:
#     # print results of select to stdout
#
#     # mask tables are not huge
#     # we can dare to fetchall
#     # that should not exhaust resources
#     results = db.cursor().fetchall()
#
#     # arrays to store the characteristics of columns in the database
#     colmyt = []
#     colname = []
#     coltype = []
#
#     # painstakingly looking at type that odbc gives to psycopg2
#     # these values have not been found in the psycopg2 docs
#     # they may not be the same on all platforms
#     # on the doc page at
#     # https://www.psycopg.org/docs/module.html
#     # it talks about hints, but there are no hints
#     # it talks about type codes in cursor object description
#     # but there is not a list of those on that doc page
#     pg2DateTime = 1114
#     pg2Int = 23
#     pg2Char = 1042
#     pg2String = 25
#     pg2Float = 700
#     pg2Double = 701
#     pg2WhatIsIt = -1
#
#     # a successful query should have retrieved the result characteristics
#     if not db.cursor.description:
#         # None should not happen if the table exists
#         log.error("The query failed -- the result has no description")
#
#         # cannot proceed to dump without a description
#         return None
#     # end if db.cursor.description
#
#     # loop over every column in the query result
#     numcols = 0
#     for column in db.cursor.description:
#         # https://github.com/mkleehammer/pyodbc/wiki/Cursor#attributes
#         # column[0] is column name or alias
#         colname.append(column[0])
#         # column[1] is type code
#         coltype.append(column[1])
#         # column[3] is internal size
#
#         if column[1] == pg2String:
#             colmyt.append(pg2String)
#         elif column[1] == pg2Char:
#             colmyt.append(pg2Char)
#         elif column[1] == pg2Int:
#             colmyt.append(pg2Int)
#         elif column[1] == pg2Float:
#             colmyt.append(pg2Float)
#         elif column[1] == pg2Double:
#             colmyt.append(pg2Double)
#         elif column[1] == pg2DateTime:
#             colmyt.append(pg2DateTime)
#         else:
#             # no idea what other data types might be in PostgreSQL
#             colmyt.append(pg2WhatIsIt)
#         # end if isinstance
#
#         log.info(f"colname {column[0]} type {column[1]} size {column[3]} "
#                  f"{colmyt[numcols]}")
#
#         # counting columns was necessary when not using dictionary cursor
#         numcols += 1
#     # end for column
#
#     # we dump to stdout
#     f = sys.stdout
#
#     colnum = 0
#     for column in db.cursor.description:
#         f.write("%s" % (colname[colnum]))
#
#         colnum+=1
#
#         if colnum < numcols:
#             f.write(", ")
#         # end if colnum
#     # end for column
#     f.write("\n")
#
#     len_results  = len(results)
#
#     print(f"len_results {len_results}")
#
#     if len_results < 1:
#         return None
#     # end if
#
#     rownum = 0
#     for row in results:
#         colnum = 0
#         for col in row:
#             orig = row[colname[colnum]]
#             if orig == None:
#                 f.write("\\N")
#             elif colmyt[colnum] == pg2String:
#                 f.write("%s" % orig)
#             elif colmyt[colnum] == pg2Char:
#                 f.write("%s" % orig)
#             elif colmyt[colnum] == pg2Int:
#                 f.write("%d" % orig)
#             elif colmyt[colnum] == pg2Float:
#                 f.write("%f" % orig)
#             elif colmyt[colnum] == pg2Double:
#                 f.write("%f" % orig)
#             elif colmyt[colnum] == pg2DateTime:
#                 f.write("%s" % orig)
#             else:
#                 f.write("%s" % orig)
#             # end if orig
#
#             colnum+=1
#
#             if colnum < numcols:
#                 f.write('\t')
#             # end if colnum
#
#         # end for col
#
#         rownum+=1
#         f.write('\n')
#
#     # end for row
#
#     # unclear if returning this will eventually cause
#     # problems because we never free the memory
#     # but using this routine may be not good for long-running process
#     return results

# end def dumpselect()

################################################

# TODO moved to the mask_constants.py
# def get_slit_constants(key_name):
#     """
#     MaskBluStatusMILLABLE = 0     # newly submitted or set for reMill
#     MaskBluStatusFLOPPY = 1     # millcode written for the mill
#     MaskBluStatusMILLED = 2     # milled and scanned as physical inventory
#     MaskBluStatusSHIPPED = 3     # moot after milling moved from UCSC to Keck
#     MaskBluStatusFORGOTTEN = 9     # marked to be deleted from database
#
#     :param key_name:
#     :type key_name:
#     :return:
#     :rtype:
#     """
#     slit_consts = {
#         'MaskBluStatusMILLABLE': 0,
#         'MaskBluStatusFLOPPY': 1,
#         'MaskBluStatusMILLED': 2,
#         'MaskBluStatusSHIPPED': 3,
#         'MaskBluStatusFORGOTTEN': 9
#     }
#
#     return slit_consts[key_name]

