#! /usr/bin/python3

# tools for access to DEIMOS multi-HDU FITS slitmask description file (MDF)

ONLYONE = 0

from astropy.io import fits
from astropy.table import Table

import sys

import logger_utils as log_fun
import validate_utils as valid_utils
from mask_validation import MaskValidation
from general_utils import commitOrRollback

from mdf_content import mdfcontent
from slitmask_queries import get_query
from mask_insert import MaskInsert
########################################################################

# to read a MDF we must
# validate that its table structure conforms to the schema
# validate that its table content is self-consistent


class mdf2dbmaps:
    """
    This object holds all of the maps which we construct
    as part of translating primary keys of the MDF into
    primary keys of the tables in the slitmask database.
    The mapping is one way from input FITS file to db, so
    each map is stored as a dictionary
    """

    # this map is to the ObId in ucolick Sybase
    # Keck will want to create a map to their KeckId
    obid = {}
    desid = {}
    bluid = {}
    dslitid = {}
    bslitid = {}
    objectid = {}

    # Keck 1 is 2, Keck 2 is 1 because DEIMOS defined the MDF scheme
    # and LRIS was added later into the MDF scheme
    teleid = {'Keck I': 2, 'Keck II' : 1}


########################################################################


########################################################################


def validate_mdf_content(keck_id, hdul, db, maps, sql_params):
    """
    hdul,   # HDU list from opening a FITS file
    db,     # connection to PgSQL database with Keck slitmask info
    maps    # mdf2dbmaps object

    examine table contents of multi-extension FITS MDF
    return FAILURE if the content violates any rules
    """
    log = log_fun.get_log()
    err_report = []

    ####################################################################
    # We need a database connection to ascertain whether MaskDesign.DesAuth
    # and MaskBlu.BluObsvr are known users.

    if db is None:
        log.error('no database object connection defined.')
        return False, err_report

    ####################################################################
    # MASK Design
    
    # assume MDF contains exactly one MaskDesign
    valid, err_report = valid_utils.mdf_table_rows(hdul, err_report, log)
    if not valid:
        return False, err_report

    maps = valid_utils.set_design_pid(db, hdul, maps, sql_params)

    ####################################################################
    # MASK BLUE

    # assume MDF contains exactly one MaskBlu
    valid, err_report = valid_utils.mask_blue_rows(hdul, err_report, log)
    if not valid:
        return False, err_report

    maps = valid_utils.set_blue_pid(db, hdul, maps, sql_params)

    ####################################################################

    validate = MaskValidation(hdul, err_report, log)

    validate.slit_number()
    # TODO update once set to Keck Id -- what is this checking??
    # # we require that MaskDesign.DesAuth / maskblue be a mailbox with valid e-mail address
    # DesAuth             = hdul['MaskDesign'].data['DesAuth'][ONLYONE]
    # DesAuthEmail        = mbox2email(DesAuth)
    # if len(DesAuthEmail) == 0:
    #     msg     = ("MaskDesign.DesAuth '%s' must contain a valid e-mail address" % DesAuth)
    #     tclog.logger.error( msg )
    #     print(msg)
    #     anom    += 1
    # else:
    #     # at time of ingest we will query this from the database
    #     maps.email[DesAuth]     = DesAuthEmail
    # end if len(DesAuthEmail)

    # TODO
    # +  # we require that MaskBlu.BluObsvr be a mailbox with a known user e-mail
    # +    BluObsvr = hdul['MaskBlu'].data['BluObsvr'][ONLYONE]
    # +    BluObsvrEmail = mbox2email(BluObsvr)
    # +
    # if len(BluObsvrEmail) == 0:
    #     +        msg = ("MaskBlu.BluObsvr '%s' must contain a valid e-mail address" % BluObsvr)

    # validate.user_email(db.maskumail)
    validate.instrument()
    validate.telescope()
    validate.date_pnt()
    validate.date_use()
    # TODO
    # +  # We require that MaskBlu.GUIname not be empty.
    # +  # For DEIMOS masks the designer must specify GUIname in DSIMULATOR.
    # +  # For LRIS masks the lsc2df program must have synthesized GUIname.
    # +    BluGUIname = hdul['MaskBlu'].data['guiname'][ONLYONE]
    # +
    # if len(BluGUIname.strip()) == 0:
    #     +        msg = ("MaskBlu.GUIname is empty")
    # +  # Note also that there are other constraints on the value of
    # +  # MaskBlu.GUIname
    # +  # Those constraints are not intrinsic to the MDF itself.
    # +  # Those constraints are extrinsic because they depend on the
    # +  # the current contents of the slitmask database.
    # +  # Those constraints are implemented in okayGUIname()
    validate.design_slits()
    validate.blue_slits()
    validate.slit_object_map()
    validate.object_catalogs()

    err_report = validate.get_err_report()

    ####################################################################

    if len(err_report) > 0:
        print(f"There are {len(err_report)} error(s). The file cannot be ingested.")
        print(err_report)
        return False, err_report

    print("No anomalies in MDF file, we can ingest this file.")

    return True, err_report


########################################################################


def validate_MDF(keck_id, hdul, db, maps, sql_params):
    """
    hdul,           # astropy FITS hdulist
    db,             # connection to slitmask database
    maps,           # mdf2dbmaps object

    attempt to read hdul
    ascertain whether it is a multi-HDU FITS mask description file (MDF)
    ascertain whether the data in the MDF satisfy all validity rules
    """
    log = log_fun.get_log()
    log.info('validating MDF')
    missing = []

    # does this FITS file contain all known HDUs?
    for hdu in mdfcontent.keys():
        if hdu not in hdul:
            missing.append(f"did not find HDU {hdu}")

    if missing:
        missing.append(f"we cannot ingest MDF, it is missing tables:")
        return False, missing

    hdu_report = []

    # loop over all HDUs in this FITS file
    for hdu in hdul:
        msg = None
        if isinstance(hdu, fits.PrimaryHDU):
            pass
        elif hdu.name not in mdfcontent:
            # not an error, but surprising if extra HDUs exist
            msg = f"unexpected EXTNAME {hdu.name}"
        elif type(hdu) not in mdfcontent[hdu.name].hdutypes:
            msg = f"hdutype {type(hdu)} for EXTNAME {hdu.name} " \
                  f"mdfcontent[hdu.name].hdutypes {mdfcontent[hdu.name].hdutypes}" \
                  f"wrong hdutype {type(hdu).__name__} for EXTNAME {hdu.name}"

        if msg:
            hdu_report.append(msg)
            log.warning(msg)

    if hdu_report:
        msg = f"There is an issue(s) with the MDF HDUs."
        msg += "\n".join([f"* {item}" for item in hdu_report])
        return False, hdu_report

    # loop over all FITS HDUs that we expect in a MDF
    for extname in mdfcontent.keys():
        # does the table structure of this HDU match our expectations?
        badtables = valid_utils.valTableExt(hdul, extname)

    if badtables:
        msg = f"we cannot ingest MDF, it has malformed tables."
        msg += "\n".join([f"* {item}" for item in badtables])
        log.error(msg)
        return False, badtables

    # validate the content
    status, err_report = validate_mdf_content(keck_id, hdul, db, maps, sql_params)

    return status, err_report


########################################################################


def ingestMDF(user_info, file, db, maps, sql_params):
    """

    file,           # path to a MDF file
    db,             # connection to slitmask database
    maps            # mdf2dbmaps object

    suppose file might be a MDF
    validate the structure and content of the file
    insert data from its FITS tables into the database
    """
    log = log_fun.get_log()

    # open the FITS file
    try:
        hdul = fits.open(file)
    except Exception as e:
        msg = f"could not open file {file}: exception: {e}"
        log.error(msg)

        return False, [msg]

    # validate the structure and content of the file
    valid, err_report = validate_MDF(user_info.keck_id, hdul, db, maps, sql_params)

    if not valid:
        err_report.append(f"did not insert because file had problems")
        return False, err_report

    if db is None:
        err_report.append("no mask user, no attempt to insert")
        return False, err_report

    ####################
    # design_id = hdul['MaskDesign'].data['DesId'][ONLYONE]

    # check for duplicate GUI name
    # mdfGUIname = hdul['MaskBlu'].data['guiname'][0]
    # guiname = okayGUIname(mdfGUIname, db, log)

    # insert = MaskInsert(keck_id, design_id, guiname, db, maps, log, err_report)
    insert = MaskInsert(user_info, hdul, db, maps, log, err_report)

    # MaskDesign -> maskdesign
    #   @validate lookup map DesAuth from e-mail to ObId as DesPId
    #   @insert   use DesPId from validate
    #   @insert   create map DesId to value of new primary key in maskdesign

    # table MaskDesign was created with
    # DesId       SERIAL                  PRIMARY KEY,
    # stamp       timestamp without time zone DEFAULT now()
    mask_design_query = get_query('mask_design_insert')

    for row in hdul['MaskDesign'].data:
        insert.mask_design(row, mask_design_query)

    ####################

    # MaskBlu -> maskblu
    #   @validate lookup map BluObsvr from e-mail to ObId as BluPId
    #   @validate lookup GUIname to see if it already exists
    #   @insert   use BluPId from validate
    #   @insert   create map BluId to value of new primary key in maskblu
    #   @insert   use DesId map from MaskDesign -> maskdesign
    #   @insert   hack GUIname to be unique

    # table MaskBlu was created with
    # BluId       SERIAL                  PRIMARY KEY,
    # stamp       timestamp without time zone DEFAULT now()
    mask_blue_query = get_query('mask_blue_insert')

    for row in hdul['MaskBlu'].data:
        insert.mask_blue(row, mask_blue_query)

    ####################

    # DesiSlits -> desislits
    #   @insert   use DesId map from MaskDesign -> maskdesign
    #   @insert   create map all dSlitId to value of new primary key in desislits

    # table DesiSlits was created with
    # dSlitId     SERIAL                  PRIMARY KEY,
    design_slit_query = get_query('design_slit_insert')

    for row in hdul['DesiSlits'].data:
        insert.design_slit(row, design_slit_query)

    ####################

    # BluSlits  -> bluslits
    #   @insert   use BluId map from MaskBlu -> maskblu
    #   @insert   use all dSlitId map from DesiSlits -> desislits
    #   @insert   create map all bSlitId to value of new primary key in bluslits
    #             but that map we never use later

    # table BluSlits was created with
    # bSlitId     SERIAL                  PRIMARY KEY
    # bad         INTEGER                 DEFAULT 0
    blue_slit_query = get_query('blue_slit_insert')

    for row in hdul['BluSlits'].data:
        insert.blue_slit(row, blue_slit_query)

    ####################

    # ObjectCat -> objects
    #   @insert   create map all ObjectId to value of new primary key in objects
    # ExtendObj -> objects
    #   only if any of those fields in Objects were not NULL
    #   @insert   use all ObjectId map from ObjectCat -> objects
    # NearObj -> objects
    #   only if any of those fields in Objects were not NULL
    #   @insert   use all ObjectId map from ObjectCat -> objects

    # table objects was created with
    # ObjectId    SERIAL                  PRIMARY KEY
    # bad         INTEGER                 DEFAULT 0
    target_query = get_query('target_insert')
    extended_target_query = get_query('extended_target_insert')
    nearby_target_query = get_query('nearby_target_insert')

    for row in hdul['ObjectCat'].data:
        result = insert.target(row, target_query)
        # TODO added check for None,  needs testing
        if not result:
            continue

        # all indicators say that astropy FITS table I/O
        # does not detect NULL values in FITS table
        # sla sees here that NULL values are reported as value 0.
        # therefore this code tests against 0.
        if (row['MajAxPA'] != 0.) or (row['MinAxis'] != 0.):
            insert.extended_target(row, extended_target_query, result)
        else:
            pass

        # all indicators say that astropy FITS table I/O
        # does not detect NULL values in FITS table
        # sla sees here that NULL values are reported as value 0.
        # therefore this code tests against 0.
        if (row['PM_RA'] != 0.) or (row['PM_Dec'] != 0.) or (row['Parallax'] != 0.):
            insert.nearby_target(row, nearby_target_query, result)
        else:
            pass

    err_report = insert.get_err_report()

    ####################

    # SlitObjMap
    #   @insert   use DesId map from MaskDesign -> maskdesign
    #   @insert   use all ObjectId map from ObjectCat -> objects
    #   @insert   use all dSlitId map from DesiSlits -> desislits
    slit_object_query = get_query('slit_target_insert')

    for row in hdul['SlitObjMap'].data:
        insert.slit_target(row, slit_object_query)

    ####################

    if len(err_report) != 0:
        err_report.append(f"We have errors before commitOrRollback")
        return False, err_report

    committed, message = commitOrRollback(db)

    if committed:
        log.info("commitOrRollback worked, db should be changed")
        success = True
    else:
        err_report.append(f"commitOrRollback failed: {message}")
        success = False

    ####################

    # clear maps before we do next MDF
    maps.obid.clear()
    maps.desid.clear()

    hdul.close()

    return success, err_report


def convertLRIStoMDF(file3path, email, date_use):
    """
    given a .file3 file created by the LRIS mask design software
    convert that to a mask design FITS

    The .file3 files have contents which are basically the same as the
    input for the Windows-based Surfcam program.
    They contain coordinates on metal for the edges of slitlets
    which are to be milled into a mask for LRIS.
    They do not contain any of the celestial coordinate metadata
    nor any of the celestial object metadata which are part of
    MDF files for DEIMOS.
    Therefore the MDF files created by this function have
    FITS tables where the records for those metadata are NULL.

    In the 2023/2024 rewrite for PostgreSQL the lsc2df program
    remains as a Tcl script which is found in Keck SVN under
    kroot/util/slitmask/xfer2keck/tcl/lsc2df

    inputs:
    file3path   path to .file3 Surfcam file from LRIS mask design software
    email       e-mail address of a known mask submitter to put into MDF
    date_use    FITS DATE* string value to put into MDF

    outputs:
    MDFfile     path to DEIMOS-like mask design FITS tables (MDF) file
    """

    # convention is that we name the output MDF file like the input .file3
    file3       = os.path.basename(file3path)
    mdfname     = None

    # Despite the 2023/2024 rewrite for PostgreSQL the lsc2df Tcl code
    # outputs some messages to stdout and stderr.
    # We expect that sometimes stdout and stderr will be useful
    # for debugging problems.
    # When we last checked the Makefile for lsc2df creates KROOT/var/lsc2df/log
    lsc2dfOut   = "@KROOT@/var/lsc2df/log/%s.out" % file3
    lsc2dferr   = "@KROOT@/var/lsc2df/log/%s.err" % file3
    # we choose 'w' with the expectation that we want to overwrite
    # any previous attempts to process the same input file
    STDOUT      = open(lsc2dfOut, 'w')
    STDERR      = open(lsc2dferr, 'w')

    # the 2023/2024 version of lsc2df code is in ../tcl
    # When we last checked the Makefile for lsc2df has BINSUB = maskpgtcl
    lsc2df      = "@RELDIR@/bin/maskpgtcl/lsc2df"

    # we are going to use subprocess.call even if we are python3
    status = subprocess.call([lsc2df,
    "%s %s %s %s" % (file3path, email, mdfname, date_use)],
    stdout=STDOUT, stderr=STDERR)
    # make sure output gets flushed
    STDOUT.close()
    STDERR.close()

    if status != 0:
        tclog.logger.error(
        "%s failed: see stdout %s and stderr %s"
        % (lsc2df, lsc2dfOut, lsc2dferr) )

        # return empty string as the path of the output file
        return ""
    # end if status

    # when we last checked the Makefile for lsc2df creates KROOT/var/lsc2df
    # we expect that lsc2df has created a MDF file with this name
    mdfOutD     = "@KROOT@/var/lsc2df"
    maskfits    = "%s/%s.fits" % (mdfOutD, file3)

    return maskfits


########################################################################


import string   # string.ascii_letters string.digits

# def okayGUIname(GUIname, db, log):
#     """
#     inputs:
#         GUIname
#             usually the value of GUIname found in a MDF which we are ingesting
#             it could be any other GUIname which we want to test for duplicates
#         db
#             connection to PgSQL database with tables of Keck slitmask info
#
#     outputs:
#         okayGUIname
#             a possibly modified variant of GUIname which is at the
#             moment not a duplicate of any existing GUIname in database
#     """
#     # There are special rules for GUIname
#     # because of constraints on how and where that is used.
#
#     # MaskBlu.GUIname comes from the DEIMOS MDF file.
#     # The mask designer gets to suggest a value.
#     # That value may be modified during or after mask ingestion.
#
#     # The original cgiTcl code ingested MaskBlu.GUIname exactly as
#     # found in the DEIMOS MDF files submitted to the cgiTcl web code.
#     # The post-delivery rewrite that added LRIS into the Sybase
#     # scheme used Tcl program lsc2df which generated a GUIname
#     # using the original name of the Autoslit .file3 file that
#     # was submitted through the cgiTcl web code.
#     # In that old SybTcl world the values of GUIname were reviewed
#     # after ingestion by a daily cron job MaskKeeper.
#
#     # In this new python code from the 2023/2024 transfer project
#     # we endeavour to modify GUIname during ingestion so that there
#     # should never be duplicate values in the PostgreSQL database.
#     # Nevertheless it remains the case that the PostgreSQL database
#     # should still be subjected to at least occasional reviews by
#     # code other than the web submission interface because there
#     # will likely be cases of mask data which are not adequately
#     # handled at the time of mask ingestion.
#
#     # When the mill operator using slitmaskpc runs the MillMasks GUI
#     # and scans the mask as milled then the MillMasks GUI will copy
#     # MaskBlu.GUIname into Mask.GUIname
#     # This duplication of data was done consciously for convenience.
#     # It means that a physical mask could have a blueprint where the
#     # GUIname values differ in those two table.
#     # It also means that in various different sections of code that
#     # interact with the slitmask database some code may use
#     # MaskBlu.GUIname while other code may use Mask.GUIname
#     # So the code can be confusing, especially in the case of the
#     # Tcl code where global variables come into existence simply by
#     # performing SQL queries.
#
#     # Note that the SQL table definition for Mask.GUIname has
#     # allowed for 12 characters, but mask ingestion web code and
#     # and mask maintenance cron jobs (e.g., MaskKeeper) have
#     # restricted the name to 8 characters.
#     # The restriction in length of GUIname is important for
#     # 1) the MillMasks program which runs on slitmaskpc during mask milling
#     # 2) the physical masks themselves into which the GUIname will be milled
#     # 3) the DEIMOS setup/observing GUIs which display the GUIname
#
#     # On DEIMOS masks the text milled onto the mask is in the
#     # unilluminated corner of the mask, and for DEIMOS masks the
#     # GUIname could be much longer than 8 characters with no problems.
#
#     # On LRIS masks there is no unilluminated region and all of the
#     # text milled onto an LRIS mask risks colliding with slitlets
#     # because LRIS and its mask design program Autoslit predate the
#     # DEIMOS slitmask database processing scheme so they are ignorant
#     # of the practical issues of humans handling piles of slitmasks
#     # which are effectively indistinguishable.
#     # For LRIS masks the GUIname dare not be longer than 8 characters.
#
#     # we require that MaskBlu.GUIname not have whitespace
#     okGUIname   = GUIname.strip()
#     okGUIwords  = okGUIname.split()
#     lenokGUI    = len(okGUIwords)
#     if lenokGUI == 0:
#         msg = ("MaskBlu.GUIname is empty")
#         log.warning( msg )
#         print(msg)
#         anom    += 1
#     elif lenokGUI > 1:
#         msg = ("MaskBlu.GUIname '%s' has embedded whitespace" % (GUIname,))
#         log.warning( msg )
#         print(msg)
#         # collapse whitespace
#         newGUIname      = ''.join(okGUIwords)
#     else:
#         #msg = ("MaskBlu.GUIname '%s' had no whitespace" % (GUIname,))
#         #log.info( msg )
#         #print(msg)
#         newGUIname      = GUIname
#     # end if
#
#     # we require that  MaskBlu.GUIname be just printable ASCII
#     # because KTL and the mill code generator only know that
#     # For DEIMOS masks that is easy because FITS only knows ASCII.
#     # For LRIS masks we need to make sure that lsc2df throws away
#     # utf8 that is not ASCII when it uses the input file name.
#
#     # we require that MaskBlu.GUIname be unique in the database
#     # originally performed by Tcl Tlib proc notifyDupNames
#     # strategy here is different than that code
#     # in order to economize on SQL calls
#     # replace final character of new GUIname with %
#     # select all existing GUIname like that
#     # Hope that not all possible final characters have
#     # already been used, and use one of those unused.
#     gnSelect    = (
#     "select GUIname from MaskBlu"
#     " where GUIname like %s"
#     ";")
#
#     print("newGUIname %s" % (newGUIname,))
#     shortGUIname        = newGUIname[:7]
#     shortGUIlike        = newGUIname[:7]+"%"
#
#     try:
#         db.cursor.execute( gnSelect, (shortGUIlike,) )
#     except Exception as e:
#         msg     = ("gnSelect failed: %s: exception class %s: %s" %
#         (db.cursor.query, e.__class__.__name__, e))
#         log.error( msg )
#         print(msg)
#         errcnt += 1
#     # end try gnSelect
#
#     ## fetch all at once and test painstakingly
#     #results = db.cursor.fetchall()
#     #lenres  = len(results)
#     #print("%s gn like '%s'" % (lenres,shortGUIlike))
#     #if lenres == 0:
#     #    # super, we can just use the given GUIname
#     #    pass
#     #else:
#     #    # append an alphanumeric suffix character and see if that is unique
#     #    for lastchar in string.ascii_letters+string.digits+"_:":
#     #        match       = 0
#     #        tryGUIname  = shortGUIname + lastchar
#     #        for i in range(0,lenres):
#     #            resi    = results[i]['guiname'].strip()
#     #            print("tryGUIname '%s' i %s gn '%s'" % (tryGUIname,i,resi))
#     #            if tryGUIname == resi:
#     #                match       = match + 1
#     #            # end if tryGUIname
#     #        # end for i
#     #        if match == 0:
#     #            print("tryGUIname did not match")
#     #            newGUIname      = tryGUIname
#     #            break
#     #        # end if match
#     #    # end for lastchar
#     ## end if lenres
#
#     # fetch one at a time and match using listyness
#     guinamelist = []
#     print("guinamelist created")
#     for resrow in db.cursor:
#         rowguiname      = resrow['guiname'].strip()
#         if rowguiname not in guinamelist:
#             guinamelist.append(rowguiname)
#         else:
#             print("GUIname '%s' has dups in db" % (rowguiname,))
#         # end if rowguiname
#     # end for resrow
#     for lastchar in string.ascii_letters+string.digits+"_:":
#         tryGUIname  = shortGUIname + lastchar
#         if tryGUIname not in guinamelist:
#             print("tryGUIname '%s' was not in guinamelist %s" % (tryGUIname,guinamelist))
#             newGUIname  = tryGUIname
#             break
#         # end if tryGUIname
#     # end for lastchar
#
#     # Note that we have a race here.
#     # If another connection to the database inserts the newGUIname
#     # that we have selected then there will still be duplicates.
#     # We are not going to try to lock the database here so we probably
#     # still need something like MaskKeeper to review the database for
#     # duplicate GUIname and any other problems that might arise.
#
#     if (newGUIname != GUIname):
#         msg = (f"we change MaskBlu.GUIname to {newGUIname}")
#         log.warning(msg)
#         print(msg)
#         GUIname = newGUIname
#     # end if we changed GUIname
#
#     return GUIname
#
# # end def okayGUIname()



########################################################################

def main():

    # test against a bunch of real and fake MDF
    testlist = [
        # 'Mask.16303.fits', 'Mask.14423.fits', 'mask1.file3.fits'
        'mask1.file3.fits'
    ]

    ################################################

    # sibling file that defines our database object
    # import wspgconn
    from wspgconn import WsPgConn
    import general_utils as gen_utils
    import argparse
    from os import path

    # python module to prompt and get passwords
    # import getpass

    keck_id = '1231'
    user_pw='NQCD869DBIHVO0J1XCFU8MLC'

    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Configuration File')
    args = parser.parse_args()
    APP_PATH = path.abspath(path.dirname(__file__))
    config, log = gen_utils.start_up(APP_PATH, config_name=args.config_file)

    # user_email, user_pw = gen_utils.get_info()
    db = WsPgConn(keck_id, user_pw)

    # if len(sys.argv) > 2:
    #     # command line may provide mask user e-mail
    #     print('usage: %s [mask user e-mail]' % (sys.argv[0]))
    #     sys.exit(1)
    # elif len(sys.argv) < 2:
    #     print('running as user with no insert privs')
    #     # in test mode we start off with no connection to database
    #     db = None
    # else:
    #     # see if we can run as a known mask user
    #     maskumail = sys.argv[1]
    #     maskupass = getpass.getpass(prompt='mask password for %s: ' % maskumail, stream=None)
    #
    #     # create our database object
    #     db = wspgconn.wsPgConn()
    #
    #     # try to connect to the database
    #     if db.sconnect(maskumail, maskupass):
    #         print("connect to database as mask user %s privs %s\n" % (db.dbuser, db.privs))
    #     else:
    #         print("failed to connect to database as mask user %s\n" % (maskumail) )
    #         sys.exit(1)
    #     # end if not connect

    # end if len argv

    ################################################

    for file in testlist:
        print(f'file {file}')
        # we need a new set of maps for each MDF
        # because the primary keys in each MDF
        # all start from 0 or 1
        maps = mdf2dbmaps()
        print(f'maps {maps}, db {db}')
        db.db_connect()

        status, err_report = ingestMDF(file, db, maps)

        msg = f'status: {status}, errors: {err_report}'
        log.info(msg)
        print(msg)

        del maps

    # end for file




# end def main()

########################################################################
if __name__ == '__main__':

    main()

########################################################################
