import sys
import datetime
import subprocess
import pymysql.cursors

from os import path

from general_utils import commitOrRollback
import logger_utils as log_fun

from general_utils import do_query, get_dict_result
from mask_constants import MASK_ADMIN


def generate_mask_descript(blue_id, exec_dir, out_dir, KROOT):
    """
    generate the multi-HDU FITS file which can be appended onto a DEIMOS image
        (or LRIS image)

    HDUs in the FITS file are tables which describe a slitmask

    The TCL function:  dbMaskOut
        lives in SVN at: kroot/util/slitmask/xfer2keck/tcl
        dbMaskOut is the same Tcl program used by the DEIMOS computers
        When DEIMOS takes exposures it appends these FITS tables after the
        image HDUs.

    :param blue_id: <str> the integer of the blueprint id
    :param exec_dir: <str> path to the tcl dbMaskOut function.
    :param out_dir: <str> path to output location.

    :return:
        mask_fits_filename <str>
            Should be the multi-HDU FITS file describing mask with blue_id.
            DEIMOS deiccd dispatcher appends this to FITS image files.
            This code proceeds to convert this to G-code for the CNC mill.

        mask_ali_filename <str>
            filename of file describing alignment hole locations on the mask.
            DEIMOS and LRIS SAT (slitmask alignment tool) uses this to refine
            telescope pointing to align the mask on sky.
    """
    log = log_fun.get_log()

    # keep track of any output from the tcl dbMaskOut
    db_mask_out = f"{KROOT}/var/dbMaskOut/log/dbMaskOut.{blue_id}.out"
    db_mask_err = f"{KROOT}/var/dbMaskOut/log/dbMaskOut.{blue_id}.err"

    stdout_file = open(db_mask_out, 'w')
    stderr_file = open(db_mask_err, 'w')

    # path to the dbMaskOut tcl executable
    dbMaskOut = f"{exec_dir}/dbMaskOut"

    # we are going to use subprocess.call even if we are python3
    status = subprocess.call([dbMaskOut, f"{blue_id}"], stdout=stdout_file,
                             stderr=stderr_file)

    # if dbMaskOut failed
    if status != 0:
        log.error(f"{dbMaskOut} failed: see output {db_mask_out} {db_mask_err}")
        return None, None

    # we expect that dbMaskOut has created files with these names
    maskfits = f"{out_dir}/Mask.{blue_id}.fits"
    aliout = f"{out_dir}/Mask.{blue_id}.ali"

    return maskfits, aliout

################################################


def maskStatus(db, blue_id, newstatus):
    """

    :param db: database connection object
    :param blue_id: <str> the integer of the blueprint id
    :param newstatus: <int> the integer representing mask status,  forgotten, etc.

    :return: <bool> True if the status was updated.
    """
    log = log_fun.get_log()

    maskStatusUpdate = "update MaskBlu set status = %s where blue_id = %s"

    try:
        db.cursor.execute(maskStatusUpdate, (newstatus, blue_id))
    except Exception as e:
        log.error(f"maskStatuUpdate failed: {db.cursor.query}: "
                  f"exception class {e.__class__.__name__}: {e}")

        return False

    log.info(f"updated blue_id {blue_id} new status {newstatus}")

    # TODO need to fix database create because null millseq is normal for new masks
    newmillseq = '  '

    maskMillseqUpdate = "update MaskBlu set millseq = %s where blue_id = %s"

    try:
        db.cursor.execute(maskMillseqUpdate, (newmillseq, blue_id))
    except Exception as e:
        log.error(f"maskMillseqUpdate failed blue_id {blue_id}: {db.cursor.query}: "
                  f"exception class {e.__class__.__name__}: {e}")

        return False

    log.info(f"blue_id {blue_id} new millseq {newmillseq}")

    status, message = commitOrRollback(db)

    if status == 0:
        print("commitOrRollback failed: %s" % (message))
        return False

    return True

################################################


def desid_to_bluid(design_id, curse):
    """
    Get the blue_id from the design_id.

    :param design_id: <str> the integer of the design id

    :return: <int> the integer of the blueprint id
    """
    if not do_query('design_to_blue', curse, (design_id,)):
        err = 'Database Error!'
        return False, err

    blue_id_results = get_dict_result(curse)
    if not blue_id_results or 'bluid' not in blue_id_results[0]:
        err = f'Database Error,  no blue id found for design ID {design_id}!'
        return False, err

    return True, blue_id_results[0]['bluid']

################################################


def bluid_to_desid(blue_id, curse):
    """
    Get the design_id from the blue_id.

    :param blue_id: <str> the integer of the blueprint id

    :return: <int> the integer of the design id
    """
    if not do_query('blue_to_design', curse, (blue_id,)):
        err = 'Database Error!'
        return False, err

    design_id_results = get_dict_result(curse)
    if not design_id_results or 'desid' not in design_id_results[0]:
        err = f'Database Error,  no design id found for blue ID {blue_id}!'
        return False, err

    return True, design_id_results[0]['desid']

################################################


def my_blueprint(user_info, db_obj, blue_id):
    """
    Is the logged in user the Blueprint Observer or Admin.

    Note:
        because this uses DesId it is possible that more than
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

    :param user_info: <obj> The object containing the logged in user information
    :param db_obj: <obj> the database object.
    :param blue_id:<str> the integer of the blueprint id

    :return: <bool> True if Blueprint Observer or Admin.
    """

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

################################################


def my_design(user_info, curse, design_id):
    """
    Is the logged in user the Design Author or Admin.

    Note:
        because this uses DesId it is possible that more than
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

    :param user_info: <obj> The object containing the logged in user information
    :param curse: <psycopg2.extensions.cursor> the database cursor
    :param design_id: <str> the integer of the design id

    :return: <bool> True if Design Author or Admin.
    """

    if user_info.user_type == MASK_ADMIN:
        return True

    params = (design_id, user_info.ob_id, design_id, user_info.ob_id)
    if not do_query('design_person', curse, params):
        return False

    results = curse.fetchall()
    len_results = len(results)

    if len_results == 0:
        return False

    return True

################################################


def my_blueprint_or_design(user_info, db_obj, blue_id):
    """
    Using the Blue Id check if the bluprint is owned by the logged in user.

    :param user_info: <obj> The object containing the logged in user information
    :param db_obj: <obj> the database object.
    :param blue_id:<str> the integer of the blueprint id

    :return: <bool> True if Design Author,  Blueprint Observer,  or Admin.
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


def mask_user_id(db_obj, user_email, sql_params):
    """
    Find the user OBID (mask user ID) from the email.  This is used in the
    admin search (search by email address) and on mask validation during the
    mask submission process.

    :param db_obj: <obj> the database object.
    :param user_email: <str> the user email address

    :return: <int> the observer ID (keck ID or legacy mask user ID)
             None - an error occurred and ID could be determined.

    """
    log = log_fun.get_log()

    userQuery = ("select ObId from Observers where email ilike %s")

    try:
        db_obj.cursor.execute(userQuery, (user_email,))
    except Exception as e:
        log.error(f"{userQuery} failed: {db_obj.cursor.query}: "
                  f"exception class {e.__class__.__name__}: {e}")
        return None

    results = db_obj.cursor.fetchall()
    lenres = len(results)

    # user_email is not in the Legacy Mask (UCO pre-2023) observer table
    if lenres < 1:
        # check the Keck Observer table,  and re-check UCO table with keck_id
        mask_id = chk_keck_observers(db_obj, user_email, sql_params, log)
        if not mask_id:
            log.warning(f"{user_email} is not a registered mask user")
            return None
    # should not be possible - email in observers database should be unique.
    elif lenres > 1:
        log.error(f"db error: > 1 mask users with email {user_email}")
        return None
    else:
        mask_id = results[0]['obid']

    return mask_id


def chk_keck_observers(psql_db_obj, user_email, sql_params, log):
    """
    Find the Mask ID,  get the observer Keck ID (keck observers table),  if
    the email is associated with a Keck Observer,  use that ID to check the
    legacy Mask IDs.

    Mask ID is defined as any Mask ID in the Legacy Mask Observer table
    (originally from UCO 2023).  If not in there,  the Keck ID is used.

    Legacy Mask ID < 1000
    Keck ID > 1000

    :param psql_db_obj:
    :type psql_db_obj:
    :param user_email:
    :type user_email:
    :param sql_params:
    :type sql_params:
    :return:
    :rtype:
    """
    query = "select * from observers where email = %s"
    params = (user_email, )

    num, results = do_sql_query(query, params, sql_params)
    if num == 0 or 'Id' not in results[0]:
        return None

    mask_id = results[0]['Id']

    userQuery = "select ObId from Observers where keckid = %s"

    # check the mask database using the keck-id to look for a legacy mask ID.
    try:
        psql_db_obj.cursor.execute(userQuery, (mask_id,))
    except Exception as e:
        log.error(f"{userQuery} failed: {psql_db_obj.cursor.query}: "
                  f"exception class {e.__class__.__name__}: {e}")
        return None

    results = psql_db_obj.cursor.fetchall()
    lenres = len(results)

    if lenres > 0:
        mask_id = results[0]['obid']

    return mask_id


def do_sql_query(query, params, sql_params):
    """
    Performs a database query, assuming protection against SQL injection.
    """
    try:
        db = 'keckOperations'
        dbhost = sql_params['server']
        user = sql_params['user']
        password = sql_params['pwd']
        conn = pymysql.connect(user=user, password=password, host=dbhost,
                               database=db, autocommit=True)
        cursor = conn.cursor(pymysql.cursors.DictCursor)
        num = cursor.execute(query, params)

        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return num, result
    except Exception as e:
        return 0, None


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


