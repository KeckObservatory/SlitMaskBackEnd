import json
import math
import zipfile
import logging
import argparse
import subprocess
from os import path
from datetime import datetime, date, timedelta

from io import StringIO, BytesIO
from psycopg2.extras import Json
from flask_cors import CORS
from flask import Flask, request, jsonify, make_response, redirect, send_file, Response

import re
from astropy import units as u
from astropy.coordinates import SkyCoord

import bad_slits
import apiutils as utils
import general_utils as gen_utils
from slitmask_queries import get_query
import admin_search_utils as search_utils

from wspgconn import WsPgConn
from ingest_fun import IngestFun
from general_utils import do_query, is_admin
from mask_constants import MASK_ADMIN, MASK_USER, MASK_LOGIN, RECENT_NDAYS, \
    USER_TYPE_STR, MaskBluStatusFORGOTTEN, MaskBluStatusMILLABLE, TOOL_DIAMETER

# set the path of the files
APP_PATH = path.abspath(path.dirname(__file__))
TEMPLATE_PATH = path.join(APP_PATH, "Templates/")
app = Flask(__name__, template_folder=TEMPLATE_PATH)

@app.after_request
def log_response_code(response):
    """
    log the reponse after each request.

    :param response: <JSON object> the http response

    :return: <JSON object> the response to return from route.
    """
    log.info(f'Response code: {response.status_code}')
    return response


def serialize_datetime(obj):
    """
    change the datetime object to a format that can be returned as JSON.

    :param obj: <object> the datetime object.

    :return: <object> the JSON serializable datetime object/
    """
    if isinstance(obj, datetime):
        return obj.isoformat()


def create_response(success=1, data={}, err='', stat=200):
    """
    The uniform JSON response used by the API routes.

    :param success: <int> 1 for success,  0 for error
    :param data: <dict> The results data.
    :param err: <str> an error message.
    :param stat: <int> the HTTP status code.

    :return: <JSON object> the response to return from route.
    """
    data = data if data is not None else []

    result_dict = {'success': success, 'data': data, 'error': err}
    response = make_response(
        json.dumps(
            result_dict, indent=2, sort_keys=False, default=serialize_datetime
        )
    )
    response.status_code = stat
    response.headers['Content-Type'] = 'application/json'

    return response


class UserInfo:
    """
    The User Information Object to store user data.
    """
    def __init__(self, db_obj, keck_id, user_type, user_email):
        self.keck_id = keck_id
        self.user_type = user_type
        self.email = user_email
        self.ob_id = self.set_mask_observer_id(db_obj)
        self.user_str = self.user_type_to_str()

    def user_type_to_str(self):
        try:
            return USER_TYPE_STR[self.user_type]
        except IndexError:
            return 'undefined'

    def set_mask_observer_id(self, db_obj):
        if not self.keck_id:
            return None

        # look for the keck_id in the mask database observers table
        db = db_obj.get_conn()
        curse = db.cursor()
        obid_query = f"select obid from observers where keckid={self.keck_id}"
        curse.execute(obid_query, None)
        results = curse.fetchall()

        # if no entry in the mask observer table,  use the keck_id
        if not results:
            # keck_id will alway be > 1000 and obid always < 1000
            return self.keck_id

        return results[0][0]


def init_api(keck_id=None):
    """
    Initialize the API,  find user information from the stored cookies.

    Passing in keck_id = 0 allows the user to be an ADMIN.

    :return: <db object, UserInfo object> the database and UserInfo objects.
            None, None - both as None on error.
    """
    if keck_id:
        # used to bypass login to allow for internal scripts to query
        db_obj = WsPgConn(keck_id)
        if not db_obj.db_connect():
            log.error(f'could not connect to database with id: {keck_id}')
            return None, None

        return db_obj, None

    userinfo = gen_utils.get_userinfo(OBS_INFO)
    if not userinfo:
        return None, None

    keck_id = userinfo['Id']
    user_email = userinfo['Email']

    db_obj = WsPgConn(keck_id)
    if not db_obj.db_connect():
        log.error(f'could not connect to database with id: {keck_id}')
        return None, None

    log.info(f"keck ID {keck_id}, user type: {db_obj.get_user_type()}")

    user_type = db_obj.get_user_type()
    user_info = UserInfo(db_obj, keck_id, user_type, user_email)
    if not user_info.keck_id:
        return None, None

    return db_obj, user_info


################################################################################
#    Mask Insert / Ingest functions
################################################################################


@app.route("/slitmask/upload-mdf", methods=['POST'])
def upload_mdf():
    """
    Upload a mask file.

    :return: <str> a message regarding the success or failure of loading a mask.
    """
    if 'mask-file' not in request.files:
        return create_response(success=0, err='No file part', stat=400)

    mdf_file = request.files['mask-file']

    if mdf_file.filename == '':
        return create_response(success=0, err='No selected MDF file', stat=400)

    db_obj, user_info = init_api()
    if not db_obj:
        return redirect(LOGIN_URL)

    in_fun = IngestFun(user_info, db_obj, OBS_INFO)
    mask_path = f"{RAW_MDF_DIR}/{mdf_file.filename}"
    success, err_report = in_fun.ingestMDF(mdf_file, mask_path)
    if not success:
        errors = "\n".join([f"• {err}" for err in err_report])
        return create_response(success=0, err=errors, stat=422)

    # the MDF data map
    maps = in_fun.get_maps()

    return_data = {}

    blue_dict = maps.bluid
    for blue_id in blue_dict.values():

        # run dbmaskout inorder to get the mask_fits file for the gcode
        try:
            maskout_files = dbmaskout_runner(blue_id)
        except Exception as err:
            log.error(f"error running dbMaskOut, {blue_id}, {err}")
            maskout_files = None

        if not maskout_files:
            msg = "error creating the mask description file"
            return create_response(success=0, err=f'{msg}', stat=401)

        mask_fits_filename = maskout_files[0]

        # create the mill / gcode files [gcodepath, f2nlogpath]
        gcode_files = gcode_runner(blue_id, mask_fits_filename)
        if not gcode_files or len(gcode_files) < 2:
            return create_response(
                success=0, stat=401,
                err=f'There was a problem checking for bad slits!'
            )

        # #####################################
        bad_align_msgs = bad_slits.mark_bad_slits(db_obj, blue_id, gcode_files[1])
        if bad_align_msgs is None:
            return create_response(
                success=0, stat=503, err='Error checking for bad slits!'
            )

        return_data = {'msg': 'Mask was ingested into the database.'}
        if bad_align_msgs:
            return_data['warning'] = bad_align_msgs

    return create_response(data=return_data)



################################################################################
#    Mask Information / retrieval functions
################################################################################


@app.route("/slitmask/mill-queue")
def get_mill_queue():
    """
    find all masks which should be milled but have not been milled
    corresponds to Tcl maskQ.cgi.sin.  Allow any user access.

    api2_3.py - getMaskMillingQueue( db )

    :return: <str> list of masks which want to be milled
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    curse = db_obj.get_dict_curse()
    if not do_query('mill', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    ordered_results = gen_utils.order_mill_queue(results)

    return create_response(data=ordered_results)


@app.route("/slitmask/calibration-masks")
def get_calibration_masks():
    """
    get the list of masks with indefinitely long life, i.e., masks with Date_Use
    in the far future,  which corresponds to Tcl maskEverlasting.cgi.sin

    api2_3.py - getStandardMasks( db )

    :return: <str> list of calibration masks
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    curse = db_obj.get_dict_curse()
    if not do_query('standard_mask', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    ordered_results = gen_utils.order_cal_inventory(results)

    return create_response(data=ordered_results)


@app.route("/slitmask/user-type")
def determine_user_type():
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    return create_response(data={'user_type': user_info.user_type_to_str()})


@app.route("/slitmask/user-mask-inventory")
def get_user_mask_inventory():
    """
    get a list of mask records for the logged-in user

    api2_3.py - def getUserMaskInventory(db)

    :return: <str> array of mask records
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    curse = db_obj.get_dict_curse()
    obid_col = gen_utils.get_obid_column(curse, OBS_INFO)
    if not obid_col:
        return create_response(success=0, err='Database Error!', stat=503)

    if not do_query('user_inventory', curse, (obid_col, user_info.ob_id, user_info.ob_id)):
        committed, msg = gen_utils.commitOrRollback(db_obj)
        log.error(f'Database Error!, commit: {committed}, msg: {msg}')
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    ordered_results = gen_utils.order_inventory(results)

    return create_response(data=ordered_results)


@app.route("/slitmask/user-file-upload-history")
def get_user_file_upload_history():
    return create_response(err='NOT IMPLEMENTED', data={})


@app.route("/slitmask/mask-plot")
def get_mask_plot():
    """
    make a plot of a mask blueprint corresponds to Tcl plotMask.cgi.sin

    api2_3.py - getMaskPlot( db, bluid )

    inputs:
        blue-id <str> primary key into table MaskBlu / blueprint ID
        design-id <str> mask design ID

    :return: <str> path to SVG file with the plot
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    blue_id = request.args.get('blue-id')
    design_id = request.args.get('design-id')

    if not blue_id and not design_id:
        return create_response(
            success=0, stat=401,
            err=f'One of blue-id or design-id are required!'
        )

    if user_info.user_type not in (MASK_ADMIN, MASK_USER):
        msg = f'User: {user_info.keck_id} with access: {user_info.user_type} ' \
              f'is Unauthorized!'
        return create_response(success=0, err=msg, stat=401)

    curse = db_obj.get_dict_curse()
    if not blue_id:
        success, blue_id = utils.desid_to_bluid(design_id, curse)
        if not success:
            return create_response(success=0, err=blue_id, stat=503)

    # confirm the user is listed as either BluPId or DesPId
    if not utils.my_blueprint_or_design(user_info, db_obj, blue_id):
        msg = f'User: {user_info.keck_id} with access: {user_info.user_type} ' \
              f'is Unauthorized to view blue print: {blue_id}!'
        return create_response(success=0, err=msg, stat=403)

    curse = db_obj.get_dict_curse()
    if not do_query('blueprint', curse, (blue_id,)):
        return create_response(success=0, err='Database Error!', stat=503)

    info_results = gen_utils.get_dict_result(curse)
    len_results = len(info_results)
    if len_results < 1:
        return create_response(success=0, stat=200,
                               err=f'No mask found with blueprint ID: {blue_id}!')

    elif len_results > 1:
        msg = f"database error: {len_results} > 1 masks with blueprint ID {blue_id}"
        log.error(msg)
        return create_response(success=0, err=msg, stat=422)

    # find slit positions
    if not do_query('slit', curse, (blue_id,)):
        return create_response(success=0, err='Database Error!', stat=503)

    slit_results = gen_utils.get_dict_result(curse)

    fname = gen_utils.generate_svg_plot(user_info, info_results, slit_results, blue_id)

    return send_file(fname[0], mimetype='image/svg+xml')


@app.route("/slitmask/user-access-level")
def get_user_access_level():
    """
    report privileges accorded to the logged-in user

    api2_3.py - getUserAccessLevel( db )

    :return: <str> the logged in user's access -- admin, etc.
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    log.info(f"user {user_info.keck_id} as {user_info.user_str}")

    return create_response(data={'access_level': user_info.user_str})


@app.route("/slitmask/extend-mask-use-date")
def extend_mask_use_date():
    """
    change the Use_Date to extend lifetime of this mask design

    api2_3.py - extendMaskUseDate( db, desid, howmany, timeunit )

    inputs:
        design-id <int> the DesignId of Blueprints to extend
        num-days <int> optional,  number of days to extend the use-date

    :return: <str> a message regarding the success of failure of the extenstion.
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    num_days = request.args.get('number-days')
    design_id = request.args.get('design-id')
    if not design_id:
        return create_response(success=0, stat=401,
                               err=f'design-id is a required parameter')

    if not num_days:
        num_days = RECENT_NDAYS

    curse = db_obj.get_dict_curse()

    if not user_info:
        return redirect(LOGIN_URL)

    if not utils.my_design(user_info, curse, design_id):
        return create_response(success=0, err='Unauthorized', stat=401)

    exists, err, stat_code = gen_utils.chk_mask_exists(curse, design_id)
    if not exists:
        return create_response(success=0, err=err, stat=stat_code)

    if not do_query('extend_update', curse, (num_days, design_id)):
        return create_response(success=0, err='Database Error!', stat=503)

    committed, msg = gen_utils.commitOrRollback(db_obj)
    if not committed:
        return create_response(success=0, err=msg, stat=503)

    log.info(f"Database updated,  extended {design_id} for {num_days}")
    msg = f'The mask design {design_id} use-date has been extended for {num_days} days'

    return create_response(data={'msg': msg})


@app.route("/slitmask/forget-mask")
def forget_mask():
    """
    Forget about a mask,  essentially mark it as deleted.

    api2_3.py - def forgetBlueprint(db, bluid)

    :return: <str> a message regarding the success of failure of the extension.
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    blue_id = request.args.get('blue-id')
    design_id = request.args.get('design-id')

    if not blue_id and not design_id:
        return create_response(success=0, stat=401,
                               err=f'One of blue-id or design-id are required!')

    if not blue_id:
        curse = db_obj.get_dict_curse()
        success, blue_id = utils.desid_to_bluid(design_id, curse)
        if not success:
            return create_response(success=0, err=blue_id, stat=503)

    if not utils.my_blueprint_or_design(user_info, db_obj, blue_id):
        return create_response(success=0, err='Unauthorized', stat=401)

    # update the mask status
    success = utils.maskStatus(db_obj, blue_id, MaskBluStatusFORGOTTEN)

    if not success:
        return create_response(success=0, err='Database Error!', stat=503)

    return create_response(data={'msg': f'Mask with blue id = {blue_id} has been forgotten'})


@app.route("/slitmask/mask-description-file")
def get_mask_description_file():
    """

    api2_3.py - getMaskFile(db, blue_id)

    generate the multi-HDU FITS file which can be appended onto
    a DEIMOS or LRIS image

    HDUs in the FITS file are tables which describe a slitmask

    blue_id       the BlueprintId of the slitmask

    outputs:
        fitsfile    path to the FITS tables file written by dbMaskOut
        alifile     path to the alignment box file written by dbMaskOut
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    blue_id = request.args.get('blue-id')
    if not blue_id:
        return create_response(success=0, stat=401,
                               err=f'blue-id is a required parameter!')

    if not utils.my_blueprint(user_info, db_obj, blue_id):
        msg = f"Unauthorized: BluId {blue_id} does not belong to {user_info.keck_id}"
        return create_response(success=0, err=f'{msg}', stat=401)

    exec_dir = f"{KROOT}/{DBMASKOUT_DIR}"
    out_dir = f"{KROOT}/var/dbMaskOut/"
    mask_fits_filename, mask_ali_filename = utils.generate_mask_descript(
        blue_id, exec_dir, out_dir, KROOT
    )

    if not mask_fits_filename:
        msg = "error creating the mask description file"
        return create_response(success=0, err=f'{msg}', stat=401)

    mdf_files = [mask_fits_filename, mask_ali_filename]

    # Create a zip file in memory to store the files
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for file_path in mdf_files:
            zip_file.write(file_path, arcname=file_path.split("/")[-1])

    zip_buffer.seek(0)

    return send_file(zip_buffer, download_name=f'mdf-files-{blue_id}.zip', as_attachment=True)


@app.route("/slitmask/mill-files")
def mill_files():
    """
    In the api specification section 2.3 this is "millMask"
    There is nothing in the original cgiTcl like "millMask"
    We believe the api document means "millFile" instead of "millMask"

    api2_3.py - def millFile( db, bluid )

    generate file of CNC mill code that will cut Blueprint bluid

    inputs:
        bluid       BlueprintId should exist in the database

    outputs:
    path to G-code file which tell CNC mill how to cut the mask
    path to .f2n file of diagnostic info about slitlets

    This python function requires invoking external programs
        dbMaskOut
            Tcl script
            CAVEAT: original code was not under source control
            CAVEAT: SVN has many snapshots of different working versions
            CAVEAT: most snapshots assume other external stuff exists
            For the version to be used with this python and PostgreSQL
            source code lives in SVN at
            kroot/util/slitmask/xfer2keck/tcl/dbMaskOut.sin
            extracts mask data from database and writes FITS file
        fits2ncc
            shell script
            source code lives in SVN at
            kroot/util/ncmill/acpncc/fits2ncc.sin
            fits2ncc invokes external program acpncc
        acpncc
            C program
            source code lives in SVN within
            kroot/util/ncmill/acpncc/
            converts FITS file into CNC code for mill
     """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    blue_id = request.args.get('blue-id')

    if not blue_id:
        return create_response(success=0, stat=401,
                               err=f'The mask blueprint ID, blue-id is required!')

    # run dbmaskout inorder to get the mask_fits file
    try:
        maskout_files = dbmaskout_runner(blue_id)
    except Exception as err:
        log.error(f"error running dbMaskOut, {blue_id}, {err}")
        maskout_files = None

    if not maskout_files:
        msg = "error creating the mask description file"
        return create_response(success=0, err=f'{msg}', stat=401)

    mask_fits_filename = maskout_files[0]

    # create the mill / gcode files
    gcode_files = gcode_runner(blue_id, mask_fits_filename)
    if not gcode_files:
        create_response(success=0, stat=401,
                        err=f'There was a problem creating the gcode files!')

    # Create an in-memory zip file to store the files
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zip_file:
        for file_path in gcode_files:
            zip_file.write(file_path, arcname=file_path.split("/")[-1])

    zip_buffer.seek(0)

    return send_file(zip_buffer, download_name=f'gcode-files-{blue_id}.zip',
                     as_attachment=True)


def dbmaskout_runner(blue_id):
    exec_dir = f"{KROOT}/{DBMASKOUT_DIR}"
    out_dir = f"{KROOT}/var/dbMaskOut/"

    mask_fits_filename, mask_ali_filename = utils.generate_mask_descript(
        blue_id, exec_dir, out_dir, KROOT
    )

    if not mask_fits_filename:
        return None

    maskout_files = [mask_fits_filename, mask_ali_filename]

    return maskout_files


def gcode_runner(blue_id, mask_fits_filename):
    # convert mask FITS file into G-code
    ncmill_path = f"{KROOT}/{NCMILL_DIR}"
    fits2ncc = f"{ncmill_path}/fits2ncc"

    # redirect stdout and stderr into these files
    STDOUT = open(f"{KROOT}/var/ncmill/log/fits2ncc.{blue_id}.out", 'w+')
    STDERR = open(f"{KROOT}/var/ncmill/log/fits2ncc.{blue_id}.err", 'w')

    # call external function fits2ncc
    status = subprocess.call(
        [fits2ncc, f"{TOOL_DIAMETER}", f"{mask_fits_filename}"],
        stdout=STDOUT, stderr=STDERR
    )

    STDERR.close()

    if status != 0:
        return None

    f2nlogpath = ''
    gcodepath = ''

    # rewind the stdout from fits2ncc script
    STDOUT.seek(0)

    for line in STDOUT:
        name, var = line.partition("=")[::2]
        if not var:
            continue
        elif name == 'gcodepath':
            gcodepath = var.strip()
        elif name == 'f2nlogpath':
            f2nlogpath = var.strip()

    if (f2nlogpath == '') or (gcodepath == ''):
        return None

    STDOUT.close()

    gcode_files = [gcodepath, f2nlogpath]

    return gcode_files


# TODO needs to be updated to email PI as well -- when ready!
@app.route("/slitmask/remill-mask")
def remill_mask():
    """
    api2_3.py - remillBlueprint( db, bluid, newdate )

    attempt to mark a mask blueprint as needs to be milled (again)
    corresponds to Tcl reMill.cgi.sin

    inputs:
    db          database object which is already connected with suitable privs
                user must have admin privs or own the Blueprint
    bluid       primary key into table MaskBlu
    newdate     new value of date_use for MaskBlu with bluid

    outputs:
        email sent to PI and slitmask-admin

    list of e-mail addresses to receive the above text
    note that all mask mail messages are always copied to mask admins

    side effects:
    MaskBlu with bluid gets
      date_use        = newdate
      status          = MILLABLE
      millseq         = null string

    this needs to know the logged-in user id for logging and for e-mail
    this needs to know the new value for date_use

    attempt to reset date_use = newdate
    presumably in the future
    and otherwise mark the blueprint as needing to be milled

    this must   trigger e-mail to the logged-in user
    this should trigger e-mail to the mask Design and Blueprint owners
    this must   trigger e-mail to the mask admins
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    blue_id = request.args.get('blue-id')
    design_id = request.args.get('design-id')
    new_use_date = request.args.get('use-date')

    if not blue_id and not design_id:
        return create_response(success=0, stat=401,
                               err=f'One of blue-id or design-id are required!')

    curse = db_obj.get_dict_curse()
    if not blue_id:
        success, blue_id = utils.desid_to_bluid(design_id, curse)
        if not success:
            return create_response(success=0, err=blue_id, stat=503)

    if not is_admin(user_info, log):
        if not utils.my_blueprint_or_design(user_info, db_obj, blue_id):
            return create_response(success=0, err='Unauthorized', stat=401)

    if not do_query('remill_set_date', curse, (new_use_date, blue_id)):
        return create_response(success=0, err='Database Error!', stat=503)

    # mark blueprint as Millable
    success = utils.maskStatus(db_obj, blue_id, MaskBluStatusMILLABLE)
    if not success:
        err = f'Database Error! Mask with blue-id={blue_id}, design-id={design_id} ' \
              f'was not able to mark mask to be re-milled'
        return create_response(success=0, stat=503, err=err)


    pi_emails = utils.get_design_owner_emails(db_obj, blue_id, design_id, OBS_INFO)

    email_list = [EMAIL_INFO['admin'], user_info.email] + pi_emails

    subject = f'Mask set to be remilled, blue-id={blue_id}'

    msg = f'Mask with blue-id={blue_id},  design-id={design_id} has been ' \
          f'marked to be remilled,  new use date={new_use_date}' \
          f'\n\nThe following email addresses have been notified: {email_list}'

    # TODO update to use email_list once ready (will email PIs)
    EMAIL_INFO['to_list'] = [EMAIL_INFO['admin']]
    utils.send_email(msg, EMAIL_INFO, subject)

    return create_response(data={'msg': msg})


################################################################################
################################################################################
# Admin-only API functions
################################################################################


@app.route("/slitmask/admin-search")
def admin_search():
    """
    Find masks by the search options,  key-value JSON of options.

    def getAdminMaskInventory( db, dict ):

    :return: <JSON object> data = the search results.
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    search_options = request.args.get('search-options')

    try:
        search_options = json.loads(search_options)
    except ValueError as err:
        log.warning('Error loading the url search-options parameters!')
        search_options = None

    if not search_options:
        return create_response(success=0, stat=401,
                               err=f'search_options is a required parameter')

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    # get the query based on the search options
    query_dict = search_utils.admin_search(search_options, db_obj, OBS_INFO)
    if query_dict['msg']:
        results = [{'results': query_dict['msg']}]
        return create_response(success=1, data=results)

    curse = db_obj.get_dict_curse()

    if not do_query(None, curse, query_dict['query_args'], query=query_dict['query']):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    ordered_results = gen_utils.order_search_results(results)

    return create_response(success=1, data=ordered_results)


@app.route("/slitmask/recently-scanned-barcodes")
def get_recently_scanned_barcodes():
    """
    report recently scanned barcodes

    api2_3.py - getRecentlyScannedBarcodes( db, sortby )

    inputs:
        sortby - how to sort the results

    :return: <JSON object> where data = array of recently scanned barcode info.
    """
    # sort-by is optional
    sort_by = request.args.get('sort-by')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    recent_date = gen_utils.get_recent_day(request)

    if sort_by == 'barcode':
        query_name = 'recent_barcode'
    else:
        # default is to sort by date the mask was scanned as milled
        query_name = 'recent'

    curse = db_obj.get_dict_curse()

    if not do_query(query_name, curse, (recent_date, )):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    return create_response(data=results)


@app.route("/slitmask/timeline-report")
def get_timeline_report():
    """
    report about recently submitted masks corresponds to Tcl timely.cgi.sin

    api2_3.py - getTimelinessReport(db, recentDays)

    inputs:
        days how many days ago is the cutoff for this report

    :return: list of the timeline information
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    recent_date = gen_utils.get_recent_day(request)

    curse = db_obj.get_dict_curse()

    if not do_query('timeline', curse, (recent_date,)):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    return create_response(data=results)


@app.route("/slitmask/all-valid-masks")
def get_all_valid_masks():
    """
    list all masks which should be in the physical inventory along with some
    data from MaskBlueprint, MaskDesign, and owner corresponds to Tcl
    goodMasks.cgi.sin

    api2_3.py - getAllValidMasks(db)

    :return: info about masks which should be stored at summit
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()
    obid_col = gen_utils.get_obid_column(curse, OBS_INFO)

    full_obs_info = gen_utils.get_observer_dict(curse, OBS_INFO)
    if not full_obs_info or not obid_col:
        return create_response(success=0, err='Database Error!', stat=503)

    if not do_query('mask_valid', curse, (obid_col, )):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    # add in the observer information
    match_dict = {observer['obid']: observer for observer in full_obs_info}

    # this returns all masks in the database
    for obs in results:
        obid = obs['obid']
        if obid in match_dict:
            obs['keckid'] = match_dict[obid]['keckid']
            obs['FirstName'] = match_dict[obid]['FirstName']
            obs['LastName'] = match_dict[obid]['LastName']
            obs['Email'] = match_dict[obid]['Email']

    return create_response(data=results)


@app.route("/slitmask/delete-mask")
def delete_mask():
    """
    update the database to delete the record with maskid

    api2_3.py - deleteMask( db, maskid )

    inputs:
        desid - DesignId should exist in the database desired become permanent

    :return: <str> message if the mask was deleted successfully or not

    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    mask_id = request.args.get('mask-id')
    if not mask_id:
        return create_response(success=0, stat=401,
                               err=f'mask-id is a required parameter')
    mask_id = int(mask_id)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()

    # check mask with design id exists
    exists, err, stat_code = gen_utils.chk_exists(curse, 'chk_mask', mask_id)
    if not exists:
        return create_response(success=0, err=err, stat=stat_code)

    # delete mask
    if not do_query('mask_delete', curse, (mask_id,)):
        return create_response(success=0, err='Database Error!', stat=503)

    # check that it was successful
    committed, msg = gen_utils.commitOrRollback(db_obj)
    if not committed:
        return create_response(success=0, err=msg, stat=503)

    return create_response(data={'msg': f'mask: {mask_id} deleted.'})


@app.route("/slitmask/set-perpetual-mask-use-date")
def set_perpetual_mask_use_date():
    """
    mark this mask design as permanent
    mark this mask design as a "standard"
    mark this mask design as not to be purged

    api2_3.py - def setPerpetualMaskUseDate( db, desid )

    inputs:
        design-id - the DesId whose Blueprints will become permanent

    :return: <str> message if the mask's date was extended successfully or not
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    design_id = request.args.get('design-id')
    if not design_id:
        return create_response(success=0, stat=401,
                               err=f'design-id is a required parameter')
    design_id = int(design_id)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()

    # check mask with design id exists
    exists, err, stat_code = gen_utils.chk_mask_exists(curse, design_id)
    if not exists:
        return create_response(success=0, err=err, stat=stat_code)

    # update the mask use date
    if not do_query('update_perpetual', curse, (design_id,)):
        return create_response(success=0, err='Database Error!', stat=503)

    committed, msg = gen_utils.commitOrRollback(db_obj)
    if not committed:
        return create_response(success=0, err=msg, stat=503)

    # if any of those blueprints has status FORGOTTEN then reset to MILLED
    if not do_query('forgotten_status', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    committed, msg = gen_utils.commitOrRollback(db_obj)
    if not committed:
        return create_response(success=0, err=msg, stat=503)

    return create_response(data={'msg': f'{design_id} mask updated.'})

# -- end Admin only
# -- long functions

@app.route("/slitmask/mask-detail")
def get_mask_detail():
    """
    get all database records related to this DesId.

    api2_3.py - def getDesignDetails(db, desid)

    inputs:
        design-id - desId should exist in the database

    :return: arrays JSON objects with of mask details
    """
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    design_id = request.args.get('design-id')
    if not design_id:
        return create_response(success=0, stat=422,
                               err=f'design-id is a required parameter')

    curse = db_obj.get_dict_curse()

    if user_info.user_type not in (MASK_ADMIN, MASK_USER):
        return create_response(success=0, stat=401,
                               err=f'{user_info.user_type} is Unauthorized!')

    if not utils.my_design(user_info, curse, design_id):
        msg = f'Unauthorized for keck_id: {user_info.keck_id} as ' \
              f'{user_info.user_type}) to view mask with Design ID: {design_id}'
        return create_response(success=0, err=msg, stat=403)

    if not do_query('design', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    # first result - mask design details
    mask_design_results = gen_utils.get_dict_result(curse)

    if not mask_design_results:
        msg = f"DesId {design_id} does not exist in table MaskDesign"
        log.warning(msg)
        return create_response(success=0, err=msg, stat=422)

    # there should be exactly one result row
    design_pid = mask_design_results[0]['despid']

    # order the results and create GUI friendly keys
    mask_design = gen_utils.order_mask_design(mask_design_results[0])

    result_list = [['Mask Design', [mask_design]]]

    ############################

    results = gen_utils.get_obs_by_maskid(curse, design_pid, OBS_INFO)
    if not results:
        return create_response(success=0, err='Database Error!', stat=503)

    if len(results) == 0:
        msg = f"DesPId {design_pid} exists in DesId {design_id} but not in table Observers"
        log.error(msg)
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += [['Mask Author', results]]

    ############################

    # query Objects

    if not do_query('objects', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += [['Slit Object Information', gen_utils.get_dict_result(curse)]]

    ############################

    # query SlitObjMap

    if not do_query('slit_obj', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += [['Slit Map', gen_utils.get_dict_result(curse)]]

    ############################

    # query DesiSlits

    if not do_query('design_slits', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += [['Design Slits', gen_utils.get_dict_result(curse)]]

    ############################

    # query MaskBlu to get all Blueprints derived from Design with DesId

    if not do_query('mask_blue', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    result_list += [['Blueprint', results]]

    for maskblurow in results:

        bluid = maskblurow['bluid']
        blupid = maskblurow['blupid']

        ########################

        # query the Blueprint Observer from Observers
        results = gen_utils.get_obs_by_maskid(curse, design_pid, OBS_INFO)
        if not results:
            return create_response(success=0, err='Database Error!', stat=503)

        if len(results) == 0:
            msg = f"BluPId {blupid} exists in BluId {bluid} but not in table Observers"
            log.error(msg)
            return create_response(success=0, err='Database Error!', stat=503)

        result_list += [['Blueprint Observers', results]]

        ########################

        # query BlueSlits

        if not do_query('blue_slit', curse, (bluid,)):
            return create_response(success=0, err='Database Error!', stat=503)

        result_list += [['Blue Slits', gen_utils.get_dict_result(curse)]]

        ########################

        # query Mask

        if not do_query('blue_mask', curse, (bluid,)):
            return create_response(success=0, err='Database Error!', stat=503)

        result_list += [['Blue Mask', gen_utils.get_dict_result(curse)]]

    return create_response(data=result_list)


################################################################################
#    Masks in the instruments
################################################################################
# TODO do not expose this route
@app.route("/slitmask/barcode-starlist", methods=['GET'])
def barcode_to_starlist():
    """
    input an array of barcodes and return a starlist with one entry per barcode.

    :return: <JSON array> one starlist line per array element
    """
    db_obj, user_info = init_api(keck_id=MASK_ADMIN)

    barcode_list_param = request.args.get('barcode-list')
    if not barcode_list_param:
        return create_response(success=0, stat=422,
                               err=f'barcode-list is a required parameter')

    # parse the JSON
    try:
        barcode_list = json.loads(barcode_list_param)
        barcode_list = [int(barcode) for barcode in barcode_list]
    except (json.JSONDecodeError, ValueError):
        return create_response(success=0, stat=400, err=f'Invalid JSON,  barcode-list.')

    curse = db_obj.get_dict_curse()

    # TODO TMP
    # barcode_list = [9645, 12769, 12799, 12772, 10840, 12778, 10180]

    starlist_info = []
    for barcode in barcode_list:
        if not do_query('barcode_to_pointing', curse, (barcode,)):
            return create_response(success=0, err='Database Error!', stat=503)

        results = gen_utils.get_dict_result(curse)
        if len(results) < 1 or 'ra_pnt' not in results[0] or 'dec_pnt' not in results[0]:
            print(f"no results found for barcode: {barcode}")
            continue

        dec_deg = results[0]['dec_pnt']
        ra_deg = results[0]['ra_pnt']
        try:
            c = SkyCoord(ra=ra_deg * u.degree, dec=dec_deg * u.degree, frame='icrs')
            c.to_string('hmsdms')
            ra_dec = re.sub(r'[hmds]', ':', c.to_string('hmsdms')).split(' ')
            results[0]['ra_pnt'] = ra_dec[0]
            results[0]['dec_pnt'] = ra_dec[1]
            starlist_info.append(results[0])
        except Exception as err:
            print(f"Error: {err}")

    date_str = datetime.utcnow().strftime('%Y%m%d')
    starlist_rows = []

    starlist_rows.append(f"#starlist generated by masks currently ({date_str}) in LRIS")
    starlist_rows.append(f"#Slitmask name   HH MM SS.SSS  DD mm ss.sss EPOCH   Rot-Mode   Position Angle ")

    for obj in starlist_info:
        line = (f"{obj['guiname']: <16} {obj['ra_pnt'].replace(':', ' ')} "
                f"{obj['dec_pnt'].replace(':', ' ')} {obj['equinpnt']} "
                f"rotmode=pa rotdest={obj['pa_pnt']}\n")
        starlist_rows.append(line)

    return create_response(data=starlist_rows)

# TODO do not expose this route
@app.route("/slitmask/guiname-starlist", methods=['GET'])
def guiname_to_starlist():
    """
    input an array of barcodes and return a starlist with one entry per barcode.

    guiname-list=["long_1.0","long_1.5","f2404_2","f2404_3","f2404_4","direct","long_0.7","RMJ1327B","GOH_LRIS","long_8.7"]

    :return: <JSON array> one starlist line per array element
    """
    db_obj, user_info = init_api(keck_id=MASK_ADMIN)

    guiname_list_param = request.args.get('guiname-list')
    if not guiname_list_param:
        return create_response(
            success=0, stat=422, err=f'guiname-list is a required parameter'
        )

    # parse the JSON
    try:
        guiname_list = json.loads(guiname_list_param)
    except (json.JSONDecodeError, ValueError):
        return create_response(
            success=0, stat=400, err=f'Invalid JSON,  guiname-list.'
        )

    curse = db_obj.get_dict_curse()

    starlist_info = []
    for guiname in guiname_list:
        if not do_query('guiname_to_pointing', curse, (guiname,)):
            return create_response(success=0, err='Database Error!', stat=503)

        results = gen_utils.get_dict_result(curse)
        if len(results) < 1 or 'ra_pnt' not in results[0] or 'dec_pnt' not in results[0]:
            print(f"no results found for guiname: {guiname}")
            continue

        dec_deg = results[0]['dec_pnt']
        ra_deg = results[0]['ra_pnt']
        try:
            c = SkyCoord(ra=ra_deg * u.degree, dec=dec_deg * u.degree, frame='icrs')
            c.to_string('hmsdms')
            ra_dec = re.sub(r'[hmds]', ':', c.to_string('hmsdms')).split(' ')
            results[0]['ra_pnt'] = ra_dec[0]
            results[0]['dec_pnt'] = ra_dec[1]
            starlist_info.append(results[0])
        except Exception as err:
            print(f"Error: {err}")

    date_str = datetime.utcnow().strftime('%Y%m%d')
    starlist_rows = []

    starlist_rows.append(f"#starlist generated by masks currently ({date_str}) in LRIS")
    starlist_rows.append(f"#Slitmask name   HH MM SS.SSS  DD mm ss.sss EPOCH   Rot-Mode   Position Angle ")

    for obj in starlist_info:
        line = (f"{obj['guiname']: <16} {obj['ra_pnt'].replace(':', ' ')} "
                f"{obj['dec_pnt'].replace(':', ' ')} {obj['equinpnt']} "
                f"rotmode=pa rotdest={obj['pa_pnt']}\n")
        starlist_rows.append(line)

    # return in starlist format instead of JSON
    starlist_fmt = "\n".join(starlist_rows)

    # return as a starlist instead of the common JSON format
    return starlist_fmt
    # return create_response(data=starlist_rows)


@app.route('/slitmask/sias', methods=["GET"])
def sias_slitmask_info():
    """

    :return: <JSON>
    """
    # bypass logging in as observer useing MASKADMIN
    db_obj, user_info = init_api(keck_id=MASK_ADMIN)

    q_type = request.args.get('type')
    date1 = request.args.get('date1')
    date2 = request.args.get('date2')
    if not date1 or not date2 or not q_type:
        return create_response(
            success=0, stat=422,
            err=f'date1, date2, and type are required parameters'
        )
    if q_type not in ('1', '2'):
        return create_response(
            success=0, stat=422, err='type must be either 1 or 2.'
        )

    output = {}
    curse = db_obj.get_dict_curse()
    q_name = f'sias_type{q_type}'

    if not do_query(q_name, curse, (date1, date2)):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    output['query'] = get_query(q_name)
    output['length'] = len(results)

    data = []
    for row in results:
        entry = {}
        for key in row.keys():
            if 'date' in key:
                entry[key] = row[key].strftime('%b %d %Y %M:%S')
            else:
                entry[key] = row[key]
        data.append(entry)

    output['status'] = 'COMPLETE'
    output['results'] = data

    return create_response(data=output)



if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Configuration File')
    args = parser.parse_args()
    config, log = gen_utils.start_up(APP_PATH, config_name=args.config_file)

    # the redirect to login if user not logged in
    LOGIN_URL = gen_utils.get_cfg(config, 'urls', 'login_url')

    # to be used with dbMaskOut to create the Fits chunks
    KROOT = gen_utils.get_cfg(config, 'tcl_locations', 'kroot')
    DBMASKOUT_DIR = gen_utils.get_cfg(config, 'tcl_locations', 'dbmaskout_path')
    NCMILL_DIR = gen_utils.get_cfg(config, 'tcl_locations', 'ncmill_path')

    OBS_INFO = {}
    OBS_INFO['info_url'] = gen_utils.get_cfg(config, 'keck_observer', 'info_url')
    OBS_INFO['cookie_url'] = gen_utils.get_cfg(config, 'keck_observer', 'cookie_url')

    EMAIL_INFO = {}
    EMAIL_INFO['from'] = gen_utils.get_cfg(config, 'email_info', 'from')
    EMAIL_INFO['admin'] = gen_utils.get_cfg(config, 'email_info', 'admin')
    EMAIL_INFO['server'] = gen_utils.get_cfg(config, 'email_info', 'server')

    GCODE_DIR = gen_utils.get_cfg(config, 'tcl_params', 'gcode_dir')

    RAW_MDF_DIR = gen_utils.get_cfg(config, 'file_store', 'raw_mdf')

    api_port = gen_utils.get_cfg(config, 'api_parameters', 'port')

    # restrict to 100 MB
    app.config['MAX_CONTENT_LENGTH'] = 100 * 1024 * 1024
    app.run(host='0.0.0.0', port=api_port)


