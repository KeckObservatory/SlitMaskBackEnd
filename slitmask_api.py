import json
import math
import logging
import argparse
from os import path
from datetime import datetime, date, timedelta

from flask import Flask, request, jsonify, make_response, redirect, send_file

from io import StringIO

import apiutils as utils
import general_utils as gen_utils
from general_utils import do_query, is_admin
import ingest_fun

# import dbutils as dbutils
from wspgconn import WsPgConn

from mask_constants import MASK_ADMIN, MASK_USER, MASK_LOGIN, RECENT_NDAYS, USER_TYPE_STR

APP_PATH = path.abspath(path.dirname(__file__))
TEMPLATE_PATH = path.join(APP_PATH, "Templates/")
app = Flask(__name__, template_folder=TEMPLATE_PATH)


#TODO
LOGIN_URL = 'https://www3build.keck.hawaii.edu/login/'


@app.after_request
def log_response_code(response):
    log.info(f'Response code: {response.status_code}')
    return response


def create_response(success=1, data={}, err='', stat=200):
    data = data if data is not None else []

    result_dict = {'success': success, 'data': data, 'error': err}
    response = make_response(jsonify(result_dict))
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

        # if no entry in the mask observer table,  use the keck_id
        if not results:
            return self.keck_id

        return results[0][0]


# TODO this needs to get keck-id from cookies / userinfo
def init_api():
    # keck_id = request.args.get('keck-id')
    # ignore-cook = request.args.get('ignore-cookies')
    #
    # if ignore-cook:
        # steve
        # keck_id = '1231' # sla
        # keck_id = '4444' # not in db

    userinfo = gen_utils.get_userinfo()
    keck_id = userinfo['Id']
    user_email = userinfo['Email']

    # todo move the URL out
    if not keck_id:
        return None, None

    db_obj = WsPgConn(keck_id)
    if not db_obj.db_connect():
        log.error(f'could not connect to database with id: {keck_id}')

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


@app.route("/slitmask/upload-mdf", methods=['POST'])
def upload_mdf():
    if 'mdf_file' not in request.files:
        return create_response(success=0, err='No file part', stat=400)

    mdf_file = request.files['file']

    if mdf_file.filename == '':
        return create_response(success=0, err='No selected MDF file', stat=400)

    db_obj, user_info = init_api()
    if not db_obj:
        return redirect(LOGIN_URL)

    maps = ingest_fun.mdf2dbmaps()
    succeeded, err_report = ingest_fun.ingestMDF(mdf_file, db_obj, maps)
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

    db = db_obj.get_conn()
    curse = db.cursor()

    if not do_query('mill', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    results = curse.fetchall()

    return create_response(data=results)


# @app.route("/slitmask/standard-masks")
@app.route("/slitmask/perpetual-masks")
def get_perpetual_masks():
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

    db = db_obj.get_conn()
    curse = db.cursor()

    if not do_query('perpetual', curse, None):
        return create_response(success=0, err='Database Error!', stat=503)

    results = curse.fetchall()

    return create_response(data=results)


@app.route("/slitmask/user-mask-inventory")
def get_user_mask_inventory():
    return create_response(err='NOT IMPLEMENTED', data={})


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

    curl "http://10.96.10.115:16815/slitmask/mask-plot?keck-id=1231&blue-id=447"
    """
    blue_id = request.args.get('blue-id')
    if not blue_id:
        return create_response(success=0, stat=401,
                               err=f'blue-id is a required parameter!')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if user_info.user_type not in (MASK_ADMIN, MASK_USER):
        msg = f'User: {user_info.keck_id} with access: {user_info.user_type} is Unauthorized!'
        log.warning(msg)
        return create_response(success=0, err=msg, stat=401)

    if not utils.my_blueprint(user_info, db_obj, blue_id):
        msg = f'User: {user_info.keck_id} with access: {user_info.user_type} ' \
              f'is Unauthorized to view blue print: {blue_id}!'
        log.warning(msg)
        return create_response(success=0, err=msg, stat=403)

    # TODO why does this check both the DesId and BluId
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

    if not do_query('slit', curse, (blue_id,)):
        return create_response(success=0, err='Database Error!', stat=503)

    slit_results = gen_utils.get_dict_result(curse)

    fname = gen_utils.create_svg(user_info, info_results, slit_results, blue_id)

    return send_file(fname, mimetype='image/svg+xml', as_attachment=True)
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
    return create_response(err='NOT IMPLEMENTED', data={})

# TODO figure out the db connection...  just want db


# TODO add email
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

    # db = db_obj.get_conn()
    # curse = db_obj.get_dict_curse()

    # mark blueprint as forgotten
    retval = utils.maskStatus(db_obj, blue_id, utils.get_slit_constants('MaskBluStatusFORGOTTEN'))

    print("%s forgetBlueprint/maskStatus: bluid %s newstatus %s" % (retword[retval], bluid, MaskBluStatusFORGOTTEN))

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

@app.route("/slitmask/mask-inventory")
def get_mask_inventory():
    return create_response(err='NOT IMPLEMENTED', data={})


@app.route("/slitmask/recently-scanned-barcodes")
def get_recently_scanned_barcodes():
    # def getRecentlyScannedBarcodes( db, sortby ):
    """
    report recently scanned barcodes
    those should be the recently manufactured masks
    corresponds to Tcl barco.cgi.sin

    api2_3.py - getRecentlyScannedBarcodes( db, sortby )

    inputs:
        sortby      how to sort the results

    outputs:
    list of recently scanned mask info

    curl http://10.96.10.115:16815/slitmask/recently-scanned-barcodes?keck-id=1231
    """
    sort_by = request.args.get('sort-by')
    if not sort_by:
        return create_response(success=0, stat=401,
                               err=f'sort-by is a required parameter')

    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    recent_date = date.today() - timedelta(days=RECENT_NDAYS)
    if sort_by == 'barcode':
        query = 'recent_barcode'
    else:
        # default is to sort by date the mask was scanned as milled
        query = 'recent'

    curse = db_obj.get_dict_curse()

    if not do_query(query, curse, (recent_date, )):
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

    try:
        recent_days = int(request.args.get('number-days'))
    except (ValueError, TypeError):
        recent_days = None

    if not is_admin(user_info, log):
        return create_response(success=0, err='Unauthorized', stat=401)

    curse = db_obj.get_dict_curse()

    if not recent_days or int(recent_days) <= 0:
        recent_days = RECENT_NDAYS

    recent_date = date.today() - timedelta(days=recent_days)

    if not do_query('timeline', curse, (recent_date,)):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)

    return create_response(data=results)


@app.route("/slitmask/all_valid_masks")
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

    inputs:
        desid       DesignId should exist in the database

    outputs:
        arrays of mask details

    curl "http://10.96.10.115:16815/slitmask/mask-detail?keck-id=1231&design-id=419"

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
        log.warning(msg)
        return create_response(success=0, err=msg, stat=403)

    if not do_query('design', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    # first result
    result_list = gen_utils.get_dict_result(curse)

    if len(result_list) == 0:
        msg = f"DesId {design_id} does not exist in table MaskDesign"
        log.warning(msg)
        return create_response(success=0, err=msg, stat=200)

    # there should be exactly one result row
    design_pid = result_list[0]['despid']

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

    result_list += results
    ############################

    # query Objects

    if not do_query('objects', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += gen_utils.get_dict_result(curse)

    ############################

    # query SlitObjMap

    if not do_query('slit_obj', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += gen_utils.get_dict_result(curse)

    ############################

    # query DesiSlits

    if not do_query('design_slits', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    result_list += gen_utils.get_dict_result(curse)

    ############################

    # query MaskBlu to get all Blueprints derived from Design with DesId

    if not do_query('mask_blue', curse, (design_id, )):
        return create_response(success=0, err='Database Error!', stat=503)

    results = gen_utils.get_dict_result(curse)
    result_list += results

    for maskblurow in results:

        bluid   = maskblurow['bluid']
        blupid  = maskblurow['blupid']

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

        result_list += results

        ########################

        # query BlueSlits
        if not do_query('blue_slit', curse, (bluid,)):
            return create_response(success=0, err='Database Error!', stat=503)

        result_list += gen_utils.get_dict_result(curse)

        ########################

        # query Mask

        if not do_query('blue_mask', curse, (bluid,)):
            return create_response(success=0, err='Database Error!', stat=503)

        result_list += gen_utils.get_dict_result(curse)
        print(result_list)

    return create_response(data=result_list)

# TODO this one needs to run some command -- untested
# @app.route("/slitmask/mask-file")
@app.route("/slitmask/generate-mdf")
def get_mask_file():
    # def getMaskFile( db, blue_id ):
    """
    the name of this in the api section 2.3 is misleading
    we should rename this to something like
    writeFITSchunk

    generate the multi-HDU FITS file
    which can be appended onto a DEIMOS image
    (or LRIS image, if Keck would let us turn on that code)
    HDUs in the FITS file are tables which describe a slitmask

    This python function requires invoking external program
        dbMaskOut
            Tcl script
            for the version to be used with this python code
            source code lives in SVN at
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

    keckid      need this to get privs
    bluid       the BlueprintId of the slitmask
    desid       No, we do not use this because
                desid is redundant; we only need and only want bluid

    outputs:
    fitsfile    path to the FITS tables file written by dbMaskOut
    alifile     path to the alignment box file written by dbMaskOut
    """
    # initialize db,  get user information,  redirect if not logged in.
    db_obj, user_info = init_api()
    if not user_info:
        return redirect(LOGIN_URL)

    if not utils.my_blueprint(user_info, bluid):
        msg = f"Unauthorized: BluId {blusid} does not belong to {user_info.keck_id}"
        log.warning(msg)
        return create_response(success=0, err=f'{msg}', stat=401)

    # despite the 2023 rewrite for PostgreSQL the dbMaskOut Tcl code
    # still retains some of its blithering to stdout and stderr
    # and we expect that sometimes those outputs will be useful
    dbMOout = f"/tmp/dbMaskOut.{bluid}.out"
    dbMOerr = f"/tmp/dbMaskOut.{bluid}.err"
    STDOUT = open(dbMOout, 'w')
    STDERR = open(dbMOerr, 'w')

    # the 2023 version of dbMaskOut is in ../tcl
    # when we last checked that Makefile has BINSUB = maskpgtcl
    dbMaskOut  = "@RELDIR@/bin/maskpgtcl/dbMaskOut"

    # we are going to use subprocess.call even if we are python3
    status = subprocess.call([dbMaskOut, f"{bluid}"], stdout=STDOUT, stderr=STDERR)
    if status != 0:
        log.error(f"{dbMaskOut} failed: see stdout {dbMOout} and stderr {dbMOerr}")

        # return empty strings as the path of the output files
        return "",""
    # end if status

    # we expect that dbMaskOut has created files with these names
    dbMaskOutD = "@RELDIR@/var/dbMaskOut"
    maskfits = "%s/Mask.%d.fits" % (dbMaskOutD, bluid)
    aliout = "%s/Mask.%d.ali"  % (dbMaskOutD, bluid)

    return maskfits, aliout


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('config_file', help='Configuration File')
    args = parser.parse_args()
    config, log = gen_utils.start_up(APP_PATH, config_name=args.config_file)

    api_port = gen_utils.get_cfg(config, 'api_parameters', 'port')
    app.run(host='0.0.0.0', port=api_port)

