import sys
import json
import requests
import configparser
import logger_utils as log_fun
import psycopg2
import psycopg2.extras

import subprocess

from collections import defaultdict
from datetime import date, timedelta, datetime
import datetime
from slitmask_queries import get_query
from flask import request
from gnuplot5 import *

import mask_constants as consts

from requests.packages.urllib3.exceptions import InsecureRequestWarning

from collections import OrderedDict


def start_up(app_path, config_name='catalog_config.ini'):
    """
    The API start up configuration.

    :param app_path: <str> the path that the api is running
    :param config_name: <str> name of the config file.

    :return: config as an object, and the initialized log.
    """
    config_filename = config_name

    config_file = f'{app_path}/{config_filename}'
    config_msg = f"Reading configuration from: {config_file}"

    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        log_dir = get_cfg(config, 'api_parameters', 'log_dir')
    except KeyError:
        log_dir = f'{app_path}/log'

    log = log_fun.configure_logger(log_dir)

    log.info("Starting SlitMask Database Flask Server.")
    log.info(config_msg)

    return config, log


def get_userinfo(obs_info):
    cooked = request.cookies

    # Suppress the InsecureRequestWarning from urllib3 (www3build has old certificate)
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
    response = requests.get(obs_info['cookie_url'], cookies=cooked, verify=False)
    userinfo = json.loads(response.content.decode('utf-8'))

    if 'Id' not in userinfo or 'Email' not in userinfo:
        return None

    return userinfo


def get_obs_by_maskid(curse, observer_id, obs_info, obid=None):
    """
    get the observer information from mask OBID,  this can be either KeckID or
    the legacy OBID.  If OBID,  need keckID to get the keck observer information
    from the Keck observer table.

    The maskid here refers to the MaskDesign.despid (this is the id of the human
    associated with the design)

    :param curse: <obj> the database cursor object.
    :param observer_id: <int> the observer ID,  either legacy OBID or KeckID
    :param obs_info: <dict> the schedule API url to query the keck (mysql) observer table.
    :param obid: <int> legacy psql OBID,   if known (used on recurse).

    :return:
    """

    try:
        observer_id = int(observer_id)
    except Exception as err:
        return None

    # mask ids > 1000 are keck IDs,  otherwise it is the obid in the mask table
    if int(observer_id) > 1000:
        url_params = f"obsid={observer_id}"
        keck_observers = get_keck_obs_info(obs_info, url_params)
        if not keck_observers:
            return None

        # add obid to results,  obid = keckid
        observer_info = keck_observers[0]
        if not obid:
            observer_info['obid'] = observer_info['keckid']
        else:
            observer_info['obid'] = obid

        return [observer_info]

    # if id is a legacy mask ID (obid) < 1000,  the keck id should also
    # be in the table.  Get keck id,  and run this routine again.
    if not do_query('keckid_from_obid', curse, (observer_id,)):
        return None

    observer_keck_id = get_dict_result(curse)

    if not observer_keck_id or 'keckid' not in observer_keck_id[0]:
        return None

    # For observers without a legacy id recurse with the keckid > 1000 and
    # setting the obid paramter with original id
    return get_obs_by_maskid(curse, observer_keck_id[0]['keckid'],
                             obs_info, obid=observer_id)


def get_observer_dict(curse, obs_info):
    """
    Get the MySQL observer table and merge this with the obid in the slitmask
    PostGreSQL observer table.  The slitmask observer table is no longer
    updated (2024) but is required for legacy masks with original obid.

    :param curse: the PostGreSQL database cursor
    :type curse: dict cursor
    :return: the observer information with columns:
        Id (keck ID), Firstname, Lastname, Email, Affiliation, AllocInst
    :rtype:
    """
    log = log_fun.get_log()

    """
    list of dicts like:  
    [...
       {
           'Affiliation': 'University of Keck',  'AllocInst': 'OTHER', 
           'Email': 'jane.doe@some_uni.edu', 'FirstName': 'Jane', 
           'Id': 9999, 'LastName': 'Doe', 'Phone': '8085551212', 
           'username': 'jdoe'
       }
    ]
    """
    keck_obs_mysql_table = get_keck_obs_info(obs_info)
    
    observer_table = []

    # if the keck observers table exists,  add the legacy table to it
    if keck_obs_mysql_table:

        # get the original observers from the slitmask db (no longer update 2024)
        if not do_query('obid_column', curse, None):
            return None

        slitmask_observers = get_dict_result(curse)

        slitmask_obs_by_keckid = {item['keckid']: item for item in slitmask_observers}

        for item in keck_obs_mysql_table:
            keckid = item['Id']
            if keckid in slitmask_obs_by_keckid:
                merged_item = {**slitmask_obs_by_keckid[keckid], **item}
            else:
                merged_item = {'obid': keckid, **item}
            observer_table.append(merged_item)
    else:
        log.error('no results from observers')
        return None

    return observer_table


def get_obid_column(curse, obs_info):
    """
    Get a list the represents the OBID column from the combine mysql observer
    table and the psql observers table.

    If no obid in the original slitmask observer table (always < 1000),
        use keckid (always > 1000)

    :return: list of slitmask observer ids
    :rtype: list
    """
    observer_table = get_observer_dict(curse, obs_info)
    if not observer_table:
        return None

    obid_column = [item['obid'] for item in observer_table]

    return obid_column


def is_admin(user_info, log):
    """
    Check if the logged in user is defined as an admin.

    :param user_info:
    :param log: <obj> the UserInfo instance

    :return: <bool> True if user is Admin
    """
    if user_info.user_type != consts.MASK_ADMIN:
        msg = f'User: {user_info.keck_id} with access level: ' \
              f'{user_info.user_type} is Unauthorized!'
        log.warning(msg)
        return False

    return True


def do_query(query_name, curse, query_params, query=None):
    log = log_fun.get_log()
    if not query:
        query = get_query(query_name)
        if not query:
            return False

    try:
        curse.execute(query, query_params)
    except Exception as err:
        log.error(f"{query_name} failed, err: {err}")
        return False

    return True


def get_dict_result(curse):
    """
    Return the results from the database cursor as a python dict.

    :param curse: <obj> the database cursor.

    :return: <dict> the database query results
    """
    if not curse.description:
        return []
    column_names = [desc[0] for desc in curse.description]
    return [dict(zip(column_names, row)) for row in curse.fetchall()]


def get_cfg(config, section, param_name):
    """
    Function used to read the config file,  and exit if key or value does not
    exist.

    :param config: <class 'configparser.ConfigParser'> the config file parser.
    :param section: <str> the section name in the config file.
    :param param_name: <str> the 'key' of the parameter within the section.
    :return: <str> the config file value for the parameter.
    """
    try:
        param_val = config[section][param_name]
    except KeyError:
        err_msg = f"Check Config file, there is no parameter name - "
        err_msg += f"section: {section} parameter name: {param_name}"
        sys.exit(err_msg)

    if not param_val:
        err_msg = f"Check Config file, there is no value for "
        err_msg += f"section: {section} parameter name: {param_name}"
        sys.exit(err_msg)

    return param_val


def chk_mask_exists(curse, design_id):
    if not do_query('chk_design', curse, (design_id,)):
        return 503, 'Database Error!'

    if not get_dict_result(curse):
        return 422, f'No mask exists with design-id: {design_id}!'

    return None, None


def chk_blue_mask_exists(curse, blue_id):
    if not do_query('mask_exists_blue', curse, (blue_id,)):
        return 503, f'Database Error! {e}'

    if not get_dict_result(curse):
        return 422, f"Mask with blue-id={blue_id} not in the database"

    return None, None


def chk_exists(curse, q_name, param_id):
    if not do_query(q_name, curse, (param_id,)):
        return False, 'Database Error!',  503

    if not get_dict_result(curse):
        return False, f'No entry in database with id: {param_id}!', 200

    return True, None, None


def commitOrRollback(db):
    """
    commit or rollback after executing a series of SQL statements

    psycopg2 executes SELECT statements immediately,
    but psycopg2 defers INSERT, UPDATE, etc. until a commit

    0 = error
    1 = success
    2 = no changes needed
    """
    log = log_fun.get_log()

    pgstatus = db.conn.get_transaction_status()

    if pgstatus == psycopg2.extensions.TRANSACTION_STATUS_UNKNOWN:
        # something bad happened to the pg connection
        log.error("TRANSACTION_STATUS_UNKNOWN")
        return 0,  "ERROR with connection to database"
    elif pgstatus == psycopg2.extensions.TRANSACTION_STATUS_INERROR:
        # need to rollback
        log.error("TRANSACTION_STATUS_INERROR, rollback")
        db.conn.rollback()
        return 0,  "ERROR with commands to database, no changes were made"
    elif pgstatus == psycopg2.extensions.TRANSACTION_STATUS_INTRANS:
        # need to commit
        log.info("TRANSACTION_STATUS_INTRANS, commit")
        try:
            db.conn.commit()
        except Exception as e:
            log.error(f"commit failed: exception class {e}")
            return 0, "ERROR failed to commit changes"
        else:
            return 1, "all requested changes are committed"
    elif pgstatus == psycopg2.extensions.TRANSACTION_STATUS_IDLE:
        # probably means we do not need to do anything
        log.info("TRANSACTION_STATUS_IDLE")
        return 2, "no database changes were requested, no action was taken"
    elif pgstatus == psycopg2.extensions.TRANSACTION_STATUS_ACTIVE:
        # does this mean we are multi-threaded
        log.warning("TRANSACTION_STATUS_ACTIVE")
        return 0, "WARNING database status was active"

    log.error("undocumented TRANSACTION_STATUS %s" % pgstatus)
    return 0, "ERROR undocumented database status"


def get_recent_day(request):
    try:
        recent_days = int(request.args.get('number-days'))
    except (ValueError, TypeError):
        recent_days = None

    if not recent_days or int(recent_days) <= 0:
        recent_days = consts.RECENT_NDAYS

    recent_date = date.today() - timedelta(days=recent_days)

    return recent_date


def generate_svg_plot(user_info, info_results, slit_results, bluid):

    # get the gnuplot functions
    gnusvg = Gnuplot5()

    def draw_slit(color):
        gnusvg.DrawSlit(color, x1, y1, x2, y2, x3, y3, x4, y4, dslitid)

    instrume = info_results[0]['instrume']
    bluname = info_results[0]['bluname']
    guiname = info_results[0]['guiname']

    gnusvg.OpenSVG(str(user_info.keck_id))

    # write the header of a gnuplot 5.4 file which can create SVG
    # get the default pixel size for the resulting SVG plot
    svgx,svgy = gnusvg.Header(instrume, bluid, bluname, guiname)

    lenres = len(slit_results)

    gnusvg.draw_mask_outline(instrume)

    if lenres > 0:

        # loop over all slitlets
        for row in slit_results:
            dslitid = row['dslitid']
            slittyp = row['slittyp']
            if row['bad']:
                slittyp = 'bad'

            # Flip the X values for DEIMOS
            x1, x2, x3, x4 = [(-row[f'slitx{i}'] if instrume == "DEIMOS"
                               else row[f'slitx{i}']) for i in range(1, 5)]

            y1, y2, y3, y4 = [row[f'slity{i}'] for i in range(1, 5)]

            slit_fun_map = {
                'bad': lambda: draw_slit('red'),
                'P': lambda: draw_slit('blue'),
                'A': lambda: draw_slit('cyan'),
                'C': lambda: gnusvg.DrawHole('green', x1, y1, x3, y3, dslitid),
                'L': lambda: draw_slit('magenta'),
                'G': lambda: draw_slit('yelloworange')
            }

            draw_slits_fun = slit_fun_map.get(slittyp, lambda: draw_slit('grey'))
            draw_slits_fun()

    # get the name of the gnuplot 5.4 input file
    svgfn = gnusvg.CloseSVG()

    # suppose that gnuplot is 5.4 and in the default path
    # use the gnuplot 5.4 input to create the SVG output
    # we are using subprocess.call even if we are python3
    subprocess.call(['gnuplot', svgfn])

    # the name of the output SVG plot file
    # the default pixel size of the SVG plot for use in HTML
    plot_file_name = svgfn.replace('gnup', 'svg')
    return plot_file_name, svgx, svgy


def get_keck_obs_info(obs_info, url_params=None):
    """
    Performs a an API query
    """
    log = log_fun.get_log()

    url = f"{obs_info['info_url']}"
    if url_params:
        url += f"?{url_params}"

    # Make a GET request to the API endpoint
    response = requests.get(url, verify=False)

    try:
        observer_dict = response.json()
        if not observer_dict:
            log.warning(f'no observer found for {url_params} {observer_dict}')
            return None
    except Exception as err:
        log.warning(f'error accessing url: {url}')
        return None

    return observer_dict


################################################################################
# functions used to name and order the table columns
################################################################################


def order_mask_design(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('instrume', 'Instrument'), ('desname', 'Design-Name'),
        ('projname', 'Project-Name'), ('ra_pnt', 'RA'), ('dec_pnt', 'DEC'),
        ('equinpnt', 'Equinox'), ('lst_pnt', 'LST-Observation'),
        ('pa_pnt', 'Postion-Angle'), ('radepnt', 'Coord-Representation'),
        ('date_pnt', 'Date-Observation'), ('desdate', 'Design-Date'),
        ('date_pnt', 'Date-Observation'), ('desnslit', 'Design-Number-Slits'),
        ('desnobj', 'Design-Object-Number'), ('descreat', 'Design-Creation'),
        ('desid', 'Design-ID'), ('masktype', 'Mask-Type'), ('despid', 'User-Id-Design'),
    ]

    return OrderedDict((new_key, results[orig_key]) for orig_key, new_key in new_keys_map)


def order_mill_queue(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('desid', 'Design-ID'), ('bluid', 'Blue-ID'),
        ('guiname', 'Mask-Name'), ('desname', 'Design-Name'),
        ('desnslit', 'Number-Slits'), ('instrume', 'Instrument'),
        ('status', 'Status'), ('millseq', 'Mill-Sequence'),
        ('date_use', 'Use-Date'), ('stamp', 'Submitted')
    ]

    return rename_keys(results, new_keys_map)


def order_inventory(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('status', 'Status'), ('desdate', 'Design-Date'),
        ('date_use', 'Date-Use'), ('stamp', 'Submitted'),
        ('projname', 'Project-Name'), ('guiname', 'Mask-Name'),
        ('desname', 'Design-Name'),
        ('desnslit', 'Number-Slits'), ('instrume', 'Instrument'),
        ('ra_pnt', 'RA'), ('dec_pnt', 'DEC'), ('radepnt', 'Coordinates'),
        ('equinpnt', 'Equinox'), ('pa_pnt', 'Position Angle'),
        ('date_pnt', 'Observation Date'), ('masktype', 'Mask-Type'),
        ('descreat', 'Design-Software'), ('desid', 'Design-ID'),
        ('despid', 'Design-PI-ID')
    ]

    return rename_keys(results, new_keys_map)


def order_cal_inventory(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('maskid', 'Mask-ID'), ('guiname', 'Name'), ('bluname', 'Blueprint-Name'),
        ('bluid', 'Blue-ID'), ('date_use', 'Date-Use'),
        ('milldate', 'Scanned'), ('instrume', 'Instrument'),
        ('instrume', 'Instrument'), ('desid', 'Design-ID')
    ]

    return rename_keys(results, new_keys_map)


def order_search_results(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('status', 'Status'), ('desdate', 'Design-Date'), ('desid', 'Design-ID'),
        ('desname', 'Design-Name'), ('guiname', 'GUI-Name'),
        ('projname', 'Project-Name'), ('ra_pnt', 'RA'), ('instrume', 'Instrument'),
        ('dec_pnt', 'Declination'),
        ('radepnt', 'System'), ('keckid', 'Keck-ID'), ('firstnm', 'First-Name'),
        ('lastnm', 'Last-Name'), ('email', 'Email'), ('institution', 'Institution'),
        ('stamp', 'Submitted'), ('millseq', 'Seq'),
    ]

    return rename_keys(results, new_keys_map)


def order_timeline_results(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('status', 'Status'), ('date_use', 'Obs Date'), ('ndays', 'Days-Notice'),
        ('desname', 'DesName'), ('guiname', 'GUIName'), ('desid', 'Design-ID'),
        ('bluid', 'Blue-ID'), ('desnslit', 'Nslits'), ('desnslit', 'Nslits'),
        ('instrume', 'Inst'), ('stamp', 'Submitted'), ('millseq', 'Seq'),
        ('milldate', 'Scanned')
    ]

    return rename_keys(results, new_keys_map)


def order_scanned_barcodes(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('status', 'Status'), ('maskid', 'Barcode'), ('milldate', 'Scanned'),
        ('guiname', 'GUIName'), ('millseq', 'Seq'),  ('desname', 'Design-Name'),
        ('desid', 'Design-ID'), ('bluid', 'Blue-ID'), ('desnslit', 'Nslits'),
        ('instrume', 'Inst'), ('date_use', 'Use_Date')
    ]

    return rename_keys(results, new_keys_map)


def order_active_masks(results):
    """
    Order and rename result columns to format for the user interface.
    """
    new_keys_map = [
        ('maskid', 'Barcode'), ('guiname', 'GUI-Name'), ('millseq', 'Seq'),
        ('date_use', 'Date-Use'),  ('status', 'Status'), ('instrume', 'Inst'),
        ('FirstName', 'First-Name'), ('LastName', 'Last-Name'), ('Email', 'Email')
    ]

    return rename_keys(results, new_keys_map)


def rename_keys(results, new_keys_map):
    """
    Rename the keys for the display on the html/js side.  Dates will be changed
    to more readable (and sortable) form:  YYYY-MM-DD.
    """
    new_results = []
    for result in results:
        new_result = OrderedDict()
        for orig_key, new_key in new_keys_map:
            # skip key if not found to avoid causing an error
            if orig_key not in result:
                continue
            val = result[orig_key]
            if isinstance(val, datetime.datetime):
                val = format_date(val)
            if 'status' in orig_key:
                # UNKNOWN should not exist in the new database (post 2024)
                val = consts.STATUS_STR[val]
            if 'millseq' in orig_key and not val:
                val = 'UNK'

            new_result[new_key] = val

        new_results.append(new_result)

    return new_results


def format_date(val):
    """
    Format the date to avoid the need to format on the JS/HTML side.
    """
    if isinstance(val, datetime.datetime):
        return val.strftime('%Y-%m-%d')
    return val


def group_by_email(data):
    """
    Used to group the recently scanned masks by the observer email to be
    used with an email to send out notifications of newly milled masks to the
    PIs.
    """

    grouped_data = defaultdict(list)

    for entry in data:
        if entry['obs']:
            email = entry['obs'][0]['Email']
        else:
            continue

        entry_data = {
            "Instrument": entry["instrume"], "GUI Name": entry["guiname"],
            "Mill Date": entry["milldate"], "Mask Barcode": entry["maskid"],
            "Blueprint Name": entry["bluname"], "Blueprint Id": entry["bluid"],
            "Design Name": entry["desname"], "Design Owner": entry['obs'][0]['Email'],
            "Design Id": entry["desid"], "Design #Slits": entry["desnslit"],
            "Use Date": entry["date_use"]
        }

        grouped_data[email].append(entry_data)

    return dict(grouped_data)


