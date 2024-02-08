#! /usr/bin/python3

# tools for access to DEIMOS multi-HDU FITS slitmask description file (MDF)

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


def validate_mdf_content(hdul, db, maps):
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

    maps = valid_utils.set_design_pid(db, hdul, maps)

    ####################################################################
    # MASK BLUE

    # assume MDF contains exactly one MaskBlu
    valid, err_report = valid_utils.mask_blue_rows(hdul, err_report, log)
    if not valid:
        return False, err_report

    maps = valid_utils.set_blue_pid(db, hdul, maps)

    ####################################################################

    validate = MaskValidation(hdul, err_report, log)

    validate.slit_number()
    # TODO update once set to Keck Id
    # validate.user_email(db.maskumail)
    validate.instrument()
    validate.telescope()
    validate.date_use()
    validate.design_slits()
    validate.blue_slits()
    validate.slit_object_map()
    validate.object_catalogs()

    err_report = validate.get_err_report()

    ####################################################################

    if len(err_report) > 0:
        print(f"MDF anomaly count = {len(err_report)}; we cannot ingest this file.")
        return False, err_report

    print("No anomalies in MDF file, we can ingest this file.")

    return False, err_report


# end def validate_mdf_content()

########################################################################

def validate_MDF(hdul, db, maps):
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
    status, err_report = validate_mdf_content(hdul, db, maps)

    return status, err_report

########################################################################


def ingestMDF(file, db, maps):
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
    valid, err_report = validate_MDF(hdul, db, maps)

    if not valid:
        err_report.append(f"did not insert because file had problems")
        return False, err_report

    if db is None:
        err_report.append("no mask user, no attempt to insert")
        return False, err_report

    ####################

    insert = MaskInsert(db, maps, log, err_report)

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

    errcnt, message = commitOrRollback(db)

    if errcnt == 0:
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

# end ingestMDF()

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
