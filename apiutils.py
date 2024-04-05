#! /usr/bin/python3

################################################

import sys
import datetime
import subprocess
from os import path

from general_utils import commitOrRollback
import logger_utils as log_fun

from general_utils import do_query, get_dict_result
from mask_constants import MASK_ADMIN


################################################
def generate_mask_descript(bluid):
    """
    generate the multi-HDU FITS file which can be appended onto a DEIMOS image
        (or LRIS image)
    HDUs in the FITS file are tables which describe a slitmask

    This python function requires invoking external program
        dbMaskOut
            Tcl script
            for the version to be used with this python code
            source code lives in SVN at:
                kroot/util/slitmask/xfer2keck/tcl
            extracts mask data from database and writes FITS file

    From time immoral the UCO/Lick web server just invoked dbMaskOut

    dbMaskOut is the same Tcl program used by the DEIMOS computers

    After the summit crew loads masks into DEIMOS the DEIMOS computers
    run dbMaskOut to create the multi-HDU FITS tables files.
    When DEIMOS takes exposures it appends these FITS tables after the
    image HDUs.  This means that the DEIMOS FITS files contain all of
    the pixels from the CCDs and all of the information about the
    slitmask through which the light travelled.

    Note that since the 2010 detector upgrade LRIS has been running
    exactly the same code as DEIMOS, so LRIS could also append the
    slitmask information to its FITS images, but Keck did not allow
    that code to be turned on.

    So this has always been just a wrapper to invoke dbMaskOut.

    The original dbMaskOut code was an unconsidered hack of a hack.
    During the 2023 mask transfer project from UCO/Lick to Keck
    inspection of this old code seemed a lot like taking a cat to the
    vet and finding all the tissues of a trilobite inside.  Rather
    than perpetuate all of the confusing and useless aspects of the
    old code the new dbMaskOut code only does what is necessary.

    :return:

        mask_fits_filename

            Should be the multi-HDU FITS file describing mask with bluid.
            DEIMOS deiccd dispatcher appends this to FITS image files.
            This code proceeds to convert this to G-code for the CNC mill.

        mask_ali_filename
            Should be the file describing alignment hole locations on the mask.
            We believe something in DEIMOS and LRIS obs setup software
            uses this to refine telescope pointing to align the mask on sky.
            See comments in dbMaskOut for details about how DEIMOS works.
            ** this means that the Slit-Alignment Tool (SAT) uses this file.

    :rtype: <str>
    """
    # despite the 2023 rewrite for PostgreSQL the dbMaskOut Tcl code
    # still retains some of its blithering to stdout and stderr
    # and we expect that sometimes those outputs will be useful
    db_mask_out = f"/tmp/dbMaskOut.{bluid}.out"
    db_mask_err = f"/tmp/dbMaskOut.{bluid}.err"
    stdout_file = open(db_mask_out, 'w')
    stderr_file = open(db_mask_err, 'w')

    # TODO update the path
    # the 2023 version of dbMaskOut is in ../tcl
    # when we last checked that Makefile has BINSUB = maskpgtcl
    # dbMaskOut = "@RELDIR@/bin/maskpgtcl/dbMaskOut"
    abs_path = path.abspath(path.dirname(__file__))
    dbMaskOut = f"{abs_path}/dbMaskOut/dbMaskOut"

    # we are going to use subprocess.call even if we are python3
    status = subprocess.call([dbMaskOut, f"{bluid}"], stdout=stdout_file,
                             stderr=stderr_file)
    if status != 0:
        log.error(f"{dbMaskOut} failed: see stdout {db_mask_out} and stderr {db_mask_err}")
        return None, None

    # we expect that dbMaskOut has created files with these names
    dbMaskOutD = "@RELDIR@/var/dbMaskOut"
    maskfits = f"{dbMaskOutD}/Mask.{bluid}.fits"
    aliout = f"{dbMaskOutD}/Mask.{bluid}.ali"

    return maskfits, aliout

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

    status, message = commitOrRollback(db)

    if status == 0:
        print("commitOrRollback failed: %s" % (message))
        return False
    # end if

    print("commitOrRollback worked, db should be changed")
    return True

# end def maskStatus()

################################################


def my_design(user_info, curse, desid):
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

    # curse = db_obj.cursor()
    params = (desid, user_info.ob_id, desid, user_info.ob_id)
    if not do_query('design_person', curse, params):
        # error with query
        return False

    results = curse.fetchall()
    len_results = len(results)

    if len_results == 0:
        # No, this is not my Design
        return False

    # Yes, this is my Design
    return True

################################################


def desid_to_bluid(design_id, curse):
    """
    Get the blue_id from the design_id.

    :param design_id:
    :type design_id:
    :return:
    :rtype:
    """
    if not do_query('design_to_blue', curse, (design_id,)):
        err = 'Database Error!'
        return False, err

    blue_id_results = get_dict_result(curse)
    if not blue_id_results or 'bluid' not in blue_id_results[0]:
        err = f'Database Error,  no blue id found for design ID {design_id}!'
        return False, err

    return True, blue_id_results[0]['bluid']


def bluid_to_desid(blue_id, curse):
    """
    Get the design_id from the blue_id.

    :param blue_id:
    :type blue_id:
    :return:
    :rtype:
    """
    if not do_query('blue_to_design', curse, (blue_id,)):
        err = 'Database Error!'
        return False, err

    design_id_results = get_dict_result(curse)
    if not design_id_results or 'desid' not in design_id_results[0]:
        err = f'Database Error,  no design id found for blue ID {blue_id}!'
        return False, err

    return True, design_id_results[0]['desid']


def my_blueprint(user_info, db_obj, blue_id):

    if user_info.user_type == MASK_ADMIN:
        return True

    curse = db_obj.get_dict_curse()
    params = (blue_id, user_info.ob_id)
    if not do_query('blue_person', curse, params):
        return False

    results = curse.fetchall()

    len_results = len(results)

    if len_results == 0:
        return False

    return True


def my_blueprint_or_design(user_info, db_obj, blue_id):
    """
    Using the Blue Id check if the bluprint in owned by the logged in user.

    :param user_info:
    :type user_info:
    :param db_obj:
    :type db_obj:
    :param blue_id:
    :type blue_id:
    :return:
    :rtype:
    """

    if user_info.user_type == MASK_ADMIN:
        return True

    # first check if the blueprint is owned
    if my_blueprint(user_info, db_obj, blue_id):
        return True

    curse = db_obj.get_dict_curse()

    # get the design id
    success, design_id = bluid_to_desid(blue_id, curse)
    if not success:
        return False

    # check the design id (desid) against despid
    params = (design_id, user_info.ob_id, design_id, user_info.ob_id)
    if not do_query('design_person', curse, params):
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


def MaskUserObId(db, useremail, log):
    """
    If we do know the e-mail address of a mask user and we need to
    know the database primary key of that user.

    The mask description FITS tables (MDF) files are created at sites
    which do not have access to the database of registered mask users.
    Therefore the MDF files transport user identity as strings which
    should be valid e-mail addresses in the FITS table columns
    MaskDesign.DesAuth and MaskBlu.BluObsvr

    Furthermore, the software which ingests MDF files requires that
    the values of MaskDesign.DesAuth and MaskBlu.BluObsvr be registered
    as known users in the slitmask database.

    Keck will have to rewrite the mask ingestion code so that it
    checks the e-mail addresses in MDF files against the Keck PI login
    database.

    Keck will have to rewrite this query so that it looks for the
    e-mail address in the Keck PI login database.

    inputs:
        db              database object
                        already connected to PgSQL with suitable privs
                        db.maskumail knows the logged-in slitmask user
        useremail       an e-mail address which may be a registered mask user
                        Note that this function exists in order to look up
                        a mask user other than the logged-in user.

    outputs:
        obid = the ObserverId
        or
        None
        For the original Sybase implementation and the PostgreSQL
        scheme used during the 2023/2024 mask transfer project obid
        means the primary key of the registered user corresponding to
        useremail as found in table Observers.
        Keck will have to rewrite this to return the primary key from
        the table of people in the Keck PI login database.
    """

    userQuery = ("select ObId from Observers where email ilike %s")

    try:
        db.cursor.execute(userQuery, (useremail,) )
    except Exception as e:
        log.error(
        "%s failed: %s: exception class %s: %s"
        % ('userQuery', db.cursor.query, e.__class__.__name__, e) )
        return None
    # end try

    results = db.cursor.fetchall()

    lenres      = len(results)

    if lenres < 1:
        # lenres < 1 means there is no Observer with email useremail
        msg     = ( "%s is not a registered mask user" % (useremail))
        log.warning( msg )
        # need to print/return other information
        print(msg)
        return None
    elif lenres > 1:
        # lenres > 1, this should be impossible according to our rules
        # because in table Observers field e-mail must be unique.
        msg     = ("db error: %s > 1 mask users with email %s" % (useremail))
        log.error( msg )
        # need to print/return other information
        print(msg)
        return None
    # end if

    # lenres == 1, row is in results[0]
    return results[0]['obid']

# end def MaskUserObId()
