import json
import math
import logging
import argparse
import subprocess
from os import path
from datetime import datetime, date, timedelta

from flask import Flask, request, jsonify, make_response, redirect, send_file

from io import StringIO

import apiutils as utils
import general_utils as gen_utils
from general_utils import do_query, is_admin
import ingest_fun

from flask_cors import CORS

# import dbutils as dbutils
from wspgconn import WsPgConn

from mask_constants import MASK_ADMIN, MASK_USER, MASK_LOGIN, RECENT_NDAYS, \
    USER_TYPE_STR, MaskBluStatusFORGOTTEN, TOOL_DIAMETER

APP_PATH = path.abspath(path.dirname(__file__))
TEMPLATE_PATH = path.join(APP_PATH, "Templates/")
app = Flask(__name__, template_folder=TEMPLATE_PATH)

# CORS(app, origins='https://www3build.keck.hawaii.edu', allow_headers='Content-Type')

#TODO
LOGIN_URL = 'https://www3build.keck.hawaii.edu/login/'


@app.after_request
def log_response_code(response):
    log.info(f'Response code: {response.status_code}')
    return response


def serialize_datetime(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()


def create_response(success=1, data={}, err='', stat=200):
    data = data if data is not None else []

    result_dict = {'success': success, 'data': data, 'error': err}
    # response = make_response(jsonify(result_dict))
    response = make_response(
        json.dumps(
            result_dict, indent=2, sort_keys=False, default=serialize_datetime
        )
    )
    response.status_code = stat
    response.headers['Content-Type'] = 'application/json'

    return response


class UserInfo:
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

        # TODO this is a hack,  need something more permenant
        # if no entry in the mask observer table,  use the keck_id
        if not results:
            # check if an obid matches the keck if (avoid already defined keck-ids)
            obid_query = f"select obid from observers where obid={self.keck_id}"
            curse.execute(obid_query, None)
            results = curse.fetchall()
            if not results:
                return self.keck_id

            return self.keck_id * 1000

        return results[0][0]


def init_api():
    userinfo = gen_utils.get_userinfo()
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
    print(f'user info: {user_info.keck_id} {user_info.ob_id}')
    if not user_info.keck_id:
        return None, None

    return db_obj, user_info


"""
################################################################################
    Mask Insert / Ingest functions
################################################################################
"""

@app.route("/slitmask/user-type")
def determine_user_type():
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    return create_response(data={'user_type': user_info.user_type_to_str()})


@app.route("/slitmask/upload-mdf", methods=['POST'])
def upload_mdf():
    if 'maskFile' not in request.files:
        return create_response(success=0, err='No file part', stat=400)

    mdf_file = request.files['maskFile']

    if mdf_file.filename == '':
        return create_response(success=0, err='No selected MDF file', stat=400)

    db_obj, user_info = init_api()
    if not db_obj:
        return redirect(LOGIN_URL)

    maps = ingest_fun.mdf2dbmaps()
    # succeeded, err_report = ingest_fun.ingestMDF(user_info.keck_id, mdf_file, db_obj, maps)
    succeeded, err_report = ingest_fun.ingestMDF(user_info, mdf_file, db_obj, maps)
    if succeeded:
        return create_response(data={'msg': 'Mask was ingested into the database.'})

    errors = "\n".join([f"• {item}" for item in err_report])

    return create_response(success=0, err=errors, stat=503)

"""
################################################################################
    Mask Information / retrieval functions
################################################################################
"""


@app.route("/slitmask/mill-queue")
def get_mill_queue():
    """
    find all masks which should be milled but have not been milled
    corresponds to Tcl maskQ.cgi.sin.  Allow any user access.

    api2_3.py - getMaskMillingQueue( db )

    inputs:
        keck-d (cookie)

    outputs:
        list of masks which want to be milled
    """
    # initialize db,  get user information,  redirect if not logged in.
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
    get the list of masks with indefinitely long life, i.e.,
    masks with Date_Use in the far future
    corresponds to Tcl maskEverlasting.cgi.sin

    api2_3.py - getStandardMasks( db )

    outputs:
    list of masks which want to be milled

    curl "http://10.96.10.115:16815/slitmask/perpetual-masks?keck-id=1231"
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()
    if not do_query('standard_mask', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    ordered_results = gen_utils.order_cal_inventory(results)

    return create_response(data=ordered_results)


@app.route("/slitmask/user-mask-inventory")
def get_user_mask_inventory():
    # def getUserMaskInventory(db):
    """
    get a list of mask records for the logged-in user

    inputs:
    db          our database object knows the logged-in user
    queryLimit  ignored because the full result will be manageable
                the caller can limit what it displays
                and the original cgiTcl implementation relied on
                an obscure feature of SybTcl

    outputs:
    array of mask records

    old web pages invoke this from idcheck button "Show Mask Inventory"
    old web pages produce this using code in inventory
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    curse = db_obj.get_dict_curse()
    if not do_query('user_inventory', curse, (user_info.ob_id, user_info.ob_id)):
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
        bluid       primary key into table MaskBlu

      outputs:
        path to SVG file with the plot
    """
    blue_id = request.args.get('blue-id')
    # if not blue_id:
    #     return create_response(success=0, stat=401,
    #                            err=f'blue-id is a required parameter!')

    design_id = request.args.get('design-id')
    # if not design_id:
    #     return create_response(success=0, stat=401,
    #                            err=f'design-id is a required parameter!')

    if not blue_id and not design_id:
        return create_response(success=0, stat=401,
                               err=f'One of blue-id or design-id are required!')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if user_info.user_type not in (MASK_ADMIN, MASK_USER):
        msg = f'User: {user_info.keck_id} with access: {user_info.user_type} ' \
              f'is Unauthorized!'
        return create_response(success=0, err=msg, stat=401)

    curse = db_obj.get_dict_curse()
    if not blue_id:
        # get the blue_id from the design_id
        if not do_query('design_to_blue', curse, (design_id,)):
            return create_response(success=0, err='Database Error!', stat=503)

        blue_id_results = gen_utils.get_dict_result(curse)
        if not blue_id_results or 'bluid' not in blue_id_results[0]:
            return create_response(
                err=f'Database Error,  no blue id found for design ID {design_id}!',
                success=0, stat=503
            )
        blue_id = blue_id_results[0]['bluid']
        print("blue id from desid", blue_id)

    # confirm the user is listed as either BluPId or DesPId
    if not utils.my_blueprint(user_info, db_obj, blue_id):
        msg = f'User: {user_info.keck_id} with access: {user_info.user_type} ' \
              f'is Unauthorized to view blue print: {blue_id}!'
        print(msg)
        return create_response(success=0, err=msg, stat=403)

    curse = db_obj.get_dict_curse()
    if not do_query('blueprint', curse, (blue_id,)):
        return create_response(success=0, err='Database Error!', stat=503)

    info_results = gen_utils.get_dict_result(curse)
    print("blue", blue_id)
    print(info_results)
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
    print(fname)

    # return send_file(fname, mimetype='image/svg+xml', as_attachment=True, filename='plot.svg')
    return send_file(fname[0], mimetype='image/svg+xml')
    # return render_template('svg_viewer.html', svg_content=svg_content)


@app.route("/slitmask/user-access-level")
def get_user_access_level():
    """
    report privileges accorded to the logged-in user

    api2_3.py - getUserAccessLevel( db )

    inputs:
    keck-id  GET parameters

    outputs: privilege accorded to the logged-in user
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    log.info(f"user {user_info.keck_id} as {user_info.user_str}")

    return create_response(data={'access_level': user_info.user_str})


# TODO untested
@app.route("/slitmask/extend-mask-use-date")
def extend_mask_use_date():
    # def extendMaskUseDate( db, desid, howmany, timeunit ):
    """
    change the Use_Date to extend lifetime of this mask design

    api2_3.py - extendMaskUseDate( db, desid, howmany, timeunit )

    inputs:
        desid       a DesignId whose Blueprints should get extended life
        num-days    number of days to extend life of mask

    outputs:
    success or failure
    """
    num_days = request.args.get('number-days')
    design_id = request.args.get('design-id')
    if not design_id:
        return create_response(success=0, stat=401,
                               err=f'design-id is a required parameter')

    if not num_days:
        num_days = RECENT_NDAYS

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()

    exists, err, stat_code = gen_utils.chk_mask_exists(curse, design_id)
    if not exists:
        return create_response(success=0, err=err, stat=stat_code)

    if not do_query('extend_update', curse, (num_days, design_id)):
        return create_response(success=0, err='Database Error!', stat=503)

    committed, msg = gen_utils.commitOrRollback(db_obj)
    if not committed:
        return create_response(success=0, err=msg, stat=503)

    log.info(f"Database updated,  extended {design_id} for {num_days}")

    return create_response(data={'msg': f'extended {design_id} for {num_days}'})


@app.route("/slitmask/forget-mask")
def forget_mask():
    # def c( db, bluid ):
    return create_response(err='NOT IMPLEMENTED', data={})


@app.route("/slitmask/mill-mask")
def mill_mask():
    """
    In the api specification section 2.3 this is "millMask"
    There is nothing in the original cgiTcl like "millMask"
    We believe the api document means "millFile" instead of "millMask"

    api2_3.py - def millFile( db, bluid )

    generate file of CNC mill code that will cut Blueprint bluid

    inputs:
    db          our database object knows the logged-in user
             user must have admin privs or own the Blueprint
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

    ############
    # we already have a function that runs getMaskFile
    # maskfits, aliout = get_mask_file(db, bluid)
    mask_fits_filename, mask_ali_filename = utils.generate_mask_descript(bluid)
    if not mask_fits_filename:
        msg = "error creating the mask description file"
        return create_response(success=0, err=f'{msg}', stat=401)

    # debug during development display the result
    # print("millFile(): getMaskFile mask_fits_filename %s mask_ali_filename %s" % (mask_fits_filename,mask_ali_filename))

    # getMaskFile returned names of output files from dbMaskOut
    # mask_fits_filename
    #     Should be the multi-HDU FITS file describing mask with bluid.
    #     DEIMOS deiccd dispatcher appends this to FITS image files.
    #     This code proceeds to convert this to G-code for the CNC mill.
    # mask_ali_filename
    #     Should be the file describing alignment hole locations on the mask.
    #     We believe something in DEIMOS and LRIS obs setup software
    #     uses this to refine telescope pointing to align the mask on sky.
    #     See comments in dbMaskOut for details about how DEIMOS works.

    if not mask_fits_filename:
        # empty string mask_fits_filename means some error occurred
        # getMaskFile() is not written to return error info
        # we reiterate this code from getMaskFile() to have access to its outputs
        # when we last checked the Makefile for dbMaskOut creates KROOT/var/dbMaskOut/log
        dbMOout = "@KROOT@/var/dbMaskOut/log/BluId%s.out" % bluid
        dbMOerr = "@KROOT@/var/dbMaskOut/log/BluId%s.err" % bluid
        log.error("%s failed: see stdout %s and stderr %s" % ('dbMaskOut', dbMOout, dbMOerr))
        return FAILURE  # getMaskFile failed

    ############
    # convert mask FITS file into G-code

    # fits2ncc is a shell script
    # fits2ncc lives in SVN under kroot/util/ncmill/acpncc
    # when we last checked the Makefile in acpncc has BINSUB = ncmill
    fits2ncc = "@RELDIR@/bin/ncmill/fits2ncc"

    # despite the 2023 rewrite for PostgreSQL
    # the fits2ncc code writes to stdout (and maybe stderr?)
    # We expect that sometimes those outputs may be useful...
    # when we last checked the Makefile in acpncc creates KROOT/var/ncmill/log
    f2ncout = "@KROOT@/var/ncmill/log/fits2ncc.%s.out" % bluid
    f2ncerr = "@KROOT@/var/ncmill/log/fits2ncc.%s.err" % bluid
    # redirect stdout and stderr into these files
    STDOUT = open(f2ncout, 'w+')
    STDERR = open(f2ncerr, 'w')

    # debug during development display the run
    # print("millFile(): call %s %d %s" % (fits2ncc,tooldiam,mask_fits_filename))

    # we are going to use subprocess.call even if we are python3
    status = subprocess.call([fits2ncc, "%d" % TOOL_DIAMETER, "%s" % mask_fits_filename], stdout=STDOUT, stderr=STDERR)
    STDOUT.close()
    STDERR.close()

    # debug during development display the result
    # print("millFile(): fits2ncc status %s" % (status,))

    if status != 0:
        log.error("%s failed: see stdout %s and stderr %s" % (fits2ncc, STDOUT, STDERR))

        return FAILURE  # fits2ncc failed
    # end if status

    ############

    # stdout from fits2ncc should contain paths to files acpncc wrote
    # f2n log file
    # f2nlogpath=<path to the f2n log file>
    # CNC mill G-code file
    # gcodepath=<path to the G-code file>

    f2nlogpath = ''
    gcodepath = ''

    # debug during development display the result
    # print("millFile(): STDOUT(f2ncout)=%s" % (f2ncout,))

    STDOUT.seek(0)

    for line in STDOUT:
        name, var = line.partition("=")[::2]
        if not var:
            continue
        elif (name == 'gcodepath'):
            gcodepath = var.strip()
        elif (name == 'f2nlogpath'):
            f2nlogpath = var.strip()  # end if
    # end for

    # debug during development display the result
    # print("millFile(): gcodepath  %s" % (gcodepath,))
    # print("millFile(): f2nlogpath %s" % (f2nlogpath,))

    if (f2nlogpath != '') and (gcodepath != ''):
        yayornay = SUCCESS
    else:
        yayornay = FAILURE  # fits2ncc did not produce gcode or f2n
    # end if

    return yayornay, gcodepath, f2nlogpath

    # end def millFile()            # named millMask in api section 2.3

    return create_response(err='NOT IMPLEMENTED', data={})


# TODO unfinished - add send email
@app.route("/slitmask/remill-mask")
def remill_mask():
    """
    attempt to mark a mask blueprint as FORGOTTEN

    api2_3.py - remillBlueprint( db, bluid, newdate )

    inputs:
        bluid       primary key into table MaskBlu

    outputs:
        text of an e-mail message which must be sent
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
    blue_id = request.args.get('blue-id')
    if not blue_id:
        return create_response(success=0, stat=401,
                               err=f'blue-id is a required parameter')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    # mark blueprint as forgotten
    retval = utils.maskStatus(db_obj, blue_id, MaskBluStatusFORGOTTEN)

    print(f"{retval} forgetBlueprint/maskStatus: bluid {bluid} "
          f"newstatus {MaskBluStatusFORGOTTEN}")

    # maskStatus did the commitOrRollback

    # here we run into a problem that has existed since inception
    # simply marking as forgotten does not remove the blueprint record
    # marking as forgotten does not remove related records in tables
    # BluSlits, MaskDesign, DesiSlits, SlitObjMap, Objects, Mask

    # TODO
    # also, if this blueprint has already been milled, simply marking
    # as forgotten does not remove that physical mask from the storage
    # bins at Keck summit.

    # so what does the actual deletion?
    # is there a cron job that tries to do the deletion?

    # inspection of the Sybase contents in 2023 showed hundreds of
    # thousands of orphan records in the database

    # so whatever it was that was supposed to take action on
    # blueprints marked forgotten has not been doing its job well

    # TODO add mail lookup for user/owner
    print("foo bar need mail to user/owner and admins here")

    return


# Admin-only API functions


# @app.route("/slitmask/admin-search", methods=['POST'])
@app.route("/slitmask/admin-search")
def admin_search():
    """
    def getAdminMaskInventory( db, dict ):

    :return:
    :rtype:
    """

    """
     get a list of mask records matching key/value pairs in dict

     Use of this function is restricted to users who are
     logged in with mask administrator privileges.

     inputs:
     db          our database object knows the logged-in user
                 user must have MaskAdmin privs
     dict        a dictionary of key/value pairs used to construct the query
                 This code only looks at a few known instances of key.
                 For one set of instances of key they are examined in list order,
                 and only the first occurrence of a key in that list is used.
                 For another set of instances of key their values are all used.
                 Note that the values of these keys are going to come from web
                 users, therefore it is imperative that when these key values are
                 used for SQL queries they are appropriately protected such that
                 it must not be possible to perform SQL injection.

     outputs:
     array of mask records

     old web pages invoke this from idcheck button "Show Me My Mask Inventory"
     old web pages produce this using cgiTcl code in inventory.cgi.sin
     """
    print('here33?')
    import admin_query_utils as search_utils

    search_options = request.args.get('search-options')
    print("options", type(search_options), search_options)

    try:
        # search_options = dict(search_options)
        search_options = json.loads(search_options)
    except ValueError as err:
        print(f"huh {err}")
        search_options = None

    print('type', type(search_options))

    # search_options = request.json
    print('search-options1', search_options)
    if not search_options:
        return create_response(success=0, stat=401,
                               err=f'search_options is a required parameter')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    print('search-options', search_options)

    query, query_args = search_utils.admin_search(search_options, db_obj)

    curse = db_obj.get_dict_curse()

    if not do_query(None, curse, query_args, query=query):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    ordered_results = gen_utils.order_search_results(results)

    return create_response(success=1, data=ordered_results)


@app.route("/slitmask/recently-scanned-barcodes")
def get_recently_scanned_barcodes():
    """
    report recently scanned barcodes
    those should be the recently manufactured masks
    corresponds to Tcl barco.cgi.sin

    api2_3.py - getRecentlyScannedBarcodes( db, sortby )

    inputs:
        sortby      how to sort the results

    outputs:
    list of recently scanned mask info
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
        days  how many days ago is the cutoff for this report

    outputs:
        list of recently submitted mask info
    """
    # initialize db,  get user information,  redirect if not logged in.
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

    outputs:
        info about masks which should be stored at summit
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    recent_date = gen_utils.get_recent_day(request)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()

    if not do_query('mask_valid', curse, (recent_date,)):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    return create_response(data=results)


@app.route("/slitmask/delete-mask")
def delete_mask():
    """
    update the database to delete the record with maskid

    api2_3.py - deleteMask( db, maskid )

    inputs:
        desid - DesignId should exist in the database desired become permanent

    outputs:
        success or failure
    """
    mask_id = request.args.get('mask-id')
    if not mask_id:
        return create_response(success=0, stat=401,
                               err=f'mask-id is a required parameter')
    mask_id = int(mask_id)

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

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
    # def setPerpetualMaskUseDate( db, desid ):
    """
    this should be called
    setPerpetualDesignUseDate
    but actually it acts on all Blueprints for a Design

    update the database to
    mark this mask design as permanent
    mark this mask design as a "standard"
    mark this mask design as not to be purged

    inputs:
        desid       a DesignId whose Blueprints will become permanent

    outputs:
    success or failure
    """
    design_id = request.args.get('design-id')
    if not design_id:
        return create_response(success=0, stat=401,
                               err=f'design-id is a required parameter')
    design_id = int(design_id)

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

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

# TODO do we need this?
# if we do,  we need to merge the 3 queries - either the design-id and blue-id can be the OB-ID
# https://www3build.keck.hawaii.edu/sandbox/lfuhrman/Slitmask/SlitMaskUsers.html
@app.route("/slitmask/mask-system-users")
def get_mask_system_users():
    # def getMaskSystemUsers( db ):
    """
    list all users who own a mask design or mask blueprint
    corresponds to Tcl directory.cgi.sin

    In the original Tcl web pages this could be run by
    any logged-in mask user.
    This code restricts the query to users with admin privs.

    The original Tcl code dumped all info about the observers.
    This python code supposes that the full observer info is
    better managed by other tools.
    This python code looks only at mask-related info.
    For each observer who owns a mask design or mask blueprint
    How many designs, and info about those
    How many blueprints, and info about those
    How many masks in inventory, and info about those

    outputs:
    info about observers who own masks
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()

    # find all users who have a MaskDesign or MaskBlueprint
    if not do_query('mask_users', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list = []
    result_list += gen_utils.get_dict_result(curse)

    # find all physical masks and info about owners of their Design or Blueprint
    if not do_query('observer_mask', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += gen_utils.get_dict_result(curse)

    # find all Designs and Blueprints with no physical mask and their owner info
    if not do_query('observer_no_mask', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += gen_utils.get_dict_result(curse)

    return create_response(data=result_list)


# -- end Admin only
# -- long functions

@app.route("/slitmask/mask-detail")
def get_mask_detail():
    # def getDesignDetails( db, desid ):
    """
    select all database records related to this DesId

    api2_3.py - def getDesignDetails(db, desid)

    inputs:
        desid       DesignId should exist in the database

    outputs:
        arrays of mask details

    curl "http://10.96.10.115:16815/slitmask/mask-detail?design-id=419"

    """

    design_id = request.args.get('design-id')
    if not design_id:
        return create_response(success=0, stat=401,
                               err=f'design-id is a required parameter')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    db = db_obj.get_conn()
    curse = db_obj.get_dict_curse()

    if user_info.user_type not in (MASK_ADMIN, MASK_USER):
        return create_response(success=0, stat=401,
                               err=f'{user_info.user_type} is Unauthorized!')

    if not utils.my_design(user_info, db, design_id):
        msg = f'Unauthorized for keck_id: {user_info.keck_id} as ' \
              f'{user_info.user_type}) to view mask with Design ID: {design_id}'
        print(msg)
        return create_response(success=0, err=msg, stat=403)

    if not do_query('design', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    # first result - mask design details
    mask_design_results = gen_utils.get_dict_result(curse)

    # if len(mask_design_results) == 0:
    if not mask_design_results:
        msg = f"DesId {design_id} does not exist in table MaskDesign"
        log.warning(msg)
        return create_response(success=0, err=msg, stat=200)

    # there should be exactly one result row
    design_pid = mask_design_results[0]['despid']

    # order the results and create GUI friendly keys
    mask_design = gen_utils.order_mask_design(mask_design_results[0])

    result_list = [['Mask Design', [mask_design]]]

    ############################

    # query the Design Author from Observers
    # TODO this one would need to query the observer database
    if not do_query('design_author_obs', curse, (design_pid, )):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

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
        # TODO this one would need to query the observer database
        if not do_query('blue_obs_obs', curse, (design_pid,)):
            return create_response(success=0, err='Database Error!', stat=503)

        results = gen_utils.get_dict_result(curse)

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


# TODO this has a sub function used by another,  pull out and add response here
# TODO this one needs to run some command -- untested
@app.route("/slitmask/mask-description-file")
def get_mask_description_file():
    """

    api2_3.py - getMaskFile(db, blue_id)

    generate the multi-HDU FITS file which can be appended onto a DEIMOS image
        (or LRIS image)
    HDUs in the FITS file are tables which describe a slitmask

    blue_id       the BlueprintId of the slitmask

    outputs:
    fitsfile    path to the FITS tables file written by dbMaskOut
    alifile     path to the alignment box file written by dbMaskOut
    """
    blue_id = request.args.get('blue-id')
    if not blue_id:
        return create_response(success=0, stat=401,
                               err=f'blue-id is a required parameter!')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not utils.my_blueprint(user_info, db_obj, blue_id):
        msg = f"Unauthorized: BluId {blue_id} does not belong to {user_info.keck_id}"
        return create_response(success=0, err=f'{msg}', stat=401)

    mask_fits_filename, mask_ali_filename = utils.generate_mask_descript(blue_id)
    if not mask_fits_filename:
        msg = "error creating the mask description file"
        return create_response(success=0, err=f'{msg}', stat=401)

    return create_response(
        data={
            'mask_fits_file': mask_fits_filename,
            'mask_ali_filename': mask_ali_filename
        }
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Configuration File')
    args = parser.parse_args()
    config, log = gen_utils.start_up(APP_PATH, config_name=args.config_file)

    api_port = gen_utils.get_cfg(config, 'api_parameters', 'port')
    app.run(host='0.0.0.0', port=api_port)

