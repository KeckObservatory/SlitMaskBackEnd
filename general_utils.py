import sys
import json
import requests
import configparser
import logger_utils as log_fun
import psycopg2
import psycopg2.extras
from psycopg2 import DatabaseError

import subprocess

from datetime import date, timedelta
from slitmask_queries import get_query
from wspgconn import WsPgConn
from flask import request
from gnuplot5 import *

from mask_constants import MASK_ADMIN, RECENT_NDAYS
from requests.packages.urllib3.exceptions import InsecureRequestWarning

# from mysql_utils import query_observers

from collections import OrderedDict

def start_up(app_path, config_name='catalog_config.ini'):
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


def get_obs_by_maskid(curse, observer_id, obs_info_url, obid=None):

    try:
        observer_id = int(observer_id)
    except Exception as err:
        return None

    # mask ids > 1000 are keck IDs,  otherwise it is the obid in the mask table
    if int(observer_id) > 1000:
        url_params = f"obsid={observer_id}"
        keck_observers = get_keck_obs_info(url_params, obs_info_url)
        if not keck_observers:
            return None

        # add obid to results,  obid = keckid
        observer_info = keck_observers[0]
        if not obid:
            observer_info['obid'] = observer_info['keckid']
        else:
            observer_info['obid'] = obid

        return [observer_info]

    # if id is a legacy mask ID (obid) < 1000
    if not do_query('keckid_from_obid', curse, (observer_id,)):
        print('here we are2')

        return None

    observer_keck_id = get_dict_result(curse)

    if not observer_keck_id or 'keckid' not in observer_keck_id[0]:
        return None

    # recurse with the keckid > 1000 and setting the obid paramter with original id
    return get_obs_by_maskid(curse, observer_keck_id[0]['keckid'],
                             obs_info_url, obid=observer_id)


def get_observer_dict(curse, obs_info_url):
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

    # keck_observers_mysql = query_observers(sql_params)

    # TODO this used to get the full table
    url_params = f"obsid={observer_id}"
    keck_observers_mysql = get_keck_obs_info(url_params, obs_info_url)

    # keck_observers_mysql = get_keck_obs_info(obs_info_url)

    observer_table = []

    # if the keck observers table exists,  add the legacy table to it
    if keck_observers_mysql:

        # get the original observers from the slitmask db (no longer update 2024)
        if not do_query('obid_column', curse, None):
            # TODO handle this exception
            raise DatabaseError("Database Error!")

        slitmask_observers = get_dict_result(curse)

        slitmask_obs_by_keckid = {item['keckid']: item for item in slitmask_observers}

        for item in keck_observers_mysql:
            keckid = item['keckid']
            if keckid in slitmask_obs_by_keckid:
                merged_item = {**slitmask_obs_by_keckid[keckid], **item}
            else:
                merged_item = {'obid': keckid, **item}
            observer_table.append(merged_item)
    else:
        log.error('no results from observers')

    return observer_table


def get_obid_column(curse, obs_info_url):
    """
    Get a list the represents the OBID column from the combine mysql observer
    table and the psql observers table.

    If no obid in the original slitmask observer table (always < 1000),
        use keckid (always > 1000)

    :return: list of slitmask observer ids
    :rtype: list
    """
    observer_table = get_observer_dict(curse, obs_info_url)
    iter = 0
    for obs_dict in observer_table:
        iter += 1
        if iter > 10:
            break

    obid_column = [item['obid'] for item in observer_table]

    return obid_column




def is_admin(user_info, log):
    if user_info.user_type != MASK_ADMIN:
        msg = f'User: {user_info.keck_id} with access level: ' \
              f'{user_info.user_type} is Unauthorized!'
        log.warning(msg)
        return False

    return True


def do_query(query_name, curse, query_params, query=None):
    log = log_fun.get_log()
    if not query:
        query = get_query(query_name)

    try:
        curse.execute(query, query_params)
    except Exception as e:
        log.error(f"{query_name} failed")
        return False

    return True


def get_dict_result(curse):
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
        return False, 'Database Error!',  503

    if not get_dict_result(curse):
        return False, f'No mask exists with design-id: {design_id}!', 200

    return True, None, None


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
        recent_days = RECENT_NDAYS

    recent_date = date.today() - timedelta(days=recent_days)

    return recent_date


# import matplotlib.pyplot as plt

# def generate_svg_plot(results):
#     import matplotlib.pyplot as plt
#
#     # Create a new figure
#     fig, ax = plt.subplots()
#
#     # Loop over all slitlets
#     for row in results:
#         bad = row['bad']
#         slittyp = row['slittyp']
#         dslitid = row['dslitid']
#         x = [row['slitx1'], row['slitx2'], row['slitx3'], row['slitx4'], row['slitx1']]
#         y = [row['slity1'], row['slity2'], row['slity3'], row['slity4'], row['slity1']]
#
#         # Draw each slitlet
#         if bad:
#             ax.plot(x, y, color='red', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#         elif slittyp == 'P':
#             ax.plot(x, y, color='blue', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#         elif slittyp == 'A':
#             ax.plot(x, y, color='cyan', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#         elif slittyp == 'C':
#             ax.fill(x, y, color='green', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#         elif slittyp == 'L':
#             ax.plot(x, y, color='magenta', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#         elif slittyp == 'G':
#             ax.plot(x, y, color = '#FFA500', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#         else:
#             ax.plot(x, y, color='grey', label=dslitid, linewidth=2)  # Adjust linewidth as needed
#
#     # Set plot title and labels if needed
#     ax.set_title('Plot Title - Change me')
#     ax.set_xlabel('X-axis Label')
#     ax.set_ylabel('Y-axis Label')
#
#     # Save the plot as an SVG file
#     svgfn = './plot.svg'
#     plt.savefig(svgfn, format='svg')
#
#     return svgfn, fig.get_size_inches()[0], fig.get_size_inches()[1]


# def generate_svg_plot(results, instrume, bluid, bluname, guiname):
def generate_svg_plot(user_info, info_results, slit_results, bluid):

    instrume = info_results[0]['instrume']
    bluname = info_results[0]['bluname']
    guiname = info_results[0]['guiname']

    gnusvg = Gnuplot5()

    gnusvg.OpenSVG(str(user_info.keck_id))

    # write the header of a gnuplot 5.4 file which can create SVG
    # get the default pixel size for the resulting SVG plot
    svgx,svgy = gnusvg.Header(instrume, bluid, bluname, guiname)

    lenres = len(slit_results)

    if lenres > 0:

        # loop over all slitlets
        for row in slit_results:
            bad = row['bad']
            slittyp = row['slittyp']
            dslitid = row['dslitid']
            x1 = row['slitx1']
            y1 = row['slity1']
            x2 = row['slitx2']
            y2 = row['slity2']
            x3 = row['slitx3']
            y3 = row['slity3']
            x4 = row['slitx4']
            y4 = row['slity4']

            # draw each slitlet
            if bad:
                # slit violated milling limits
                gnusvg.DrawSlit('red', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)
            elif slittyp == 'P':
                # normal program target slit
                gnusvg.DrawSlit('blue', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)
            elif slittyp == 'A':
                # alignment star hole
                gnusvg.DrawSlit('cyan', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)
            elif slittyp == 'C':
                # circular hole
                # Grid Of Holes (GOH) metrology mask
                # World Coordinate System (WCS) metrology mask
                gnusvg.DrawHole('green', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)
            elif slittyp == 'L':
                # one segment of LRIS zigzag line hole
                # one segment of LRIS circular arc hole
                gnusvg.DrawSlit('magenta', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)
            elif slittyp == 'G':
                # DSIMULATOR "ghost" hole (not seen since initial deployment)
                # seen in the Variable Length Long Mirror Slit eternal calibration mask
                gnusvg.DrawSlit('yelloworange', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)
            else:
                # this should not happen
                # once upon a time there was "M" for LRIS pickoff mirror
                gnusvg.DrawSlit('grey', x1, y1, x2, y2, x3, y3, x4, y4, dslitid)  # end if  # end for row

    # end if

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

# TODO add order observer information

def order_mask_design(results):

    # define the order and make keys GUI friendly
    new_keys_map = [
        ('instrume', 'Instrument'), ('desname', 'Design-Name'), ('projname', 'Project-Name'),
        ('ra_pnt', 'RA'), ('dec_pnt', 'DEC'), ('equinpnt', 'Equinox'),
        ('lst_pnt', 'LST-Observation'), ('pa_pnt', 'Postion-Angle'), ('radepnt', 'Coord-Representation'),
        ('date_pnt', 'Date-Observation'), ('desdate', 'Design-Date'), ('date_pnt', 'Date-Observation'),
        ('desnslit', 'Slit-Design-Number'), ('desnobj', 'Design-Object-Number'), ('descreat', 'Design-Creation'),
        ('desid', 'Design-ID'), ('masktype', 'Mask-Type'), ('despid', 'User-Id-Design'),
    ]

    return OrderedDict((new_key, results[orig_key]) for orig_key, new_key in new_keys_map)


def order_mill_queue(results):

    # define the order and make keys GUI friendly
    new_keys_map = [
        ('desid', 'Design-ID'), ('bluid', 'Blue-ID'),
        ('guiname', 'Mask-Name'), ('desname', 'Design-Name'),
        ('desnslit', 'Number-Slits'), ('instrume', 'Instrument'),
        ('status', 'Status'), ('millseq', 'Mill-Sequence'),
        ('date_use', 'Use-Date'), ('stamp', 'Time-Stamp')
    ]

    new_results = []
    for result in results:
        new_results.append(OrderedDict((new_key, result[orig_key]) for orig_key, new_key in new_keys_map))

    return new_results


def order_inventory(results):
    new_keys_map = [
        ('desid', 'Design-ID'), ('despid', 'Design-PI-ID'), #('uid', 'User ID'),
        ('projname', 'Project-Name'), ('desname', 'Design-Name'),
        ('desnslit', 'Number-Slits'), ('desnobj', 'Number-Objects'),
        ('instrume', 'Instrument'), ('ra_pnt', 'RA'), ('dec_pnt', 'DEC'),
        ('radepnt', 'Coordinates'), ('equinpnt', 'Equinox'),
        ('pa_pnt', 'Position Angle'), ('lst_pnt', 'LST'),
        ('date_pnt', 'Observation Date'), ('masktype', 'Mask-Type'),
        ('descreat', 'Design-Creation'), ('desdate', 'Design-Date'),
        ('stamp', 'Time-Stamp')
    ]

    new_results = []
    for result in results:
        new_results.append(OrderedDict((new_key, result[orig_key]) for orig_key, new_key in new_keys_map))

    return new_results


def order_cal_inventory(results):
    new_keys_map = [
        ('maskid', 'Mask-ID'), ('guiname', 'Name'), ('bluname', 'Blueprint-Name'),
        ('bluid', 'Blue-ID'), ('date_use', 'Date-Use'),
        ('milldate', 'Mill-Date'), ('instrume', 'Instrument'),
        ('instrume', 'Instrument'), ('desid', 'Design-ID')
    ]

    new_results = []
    for result in results:
        new_results.append(OrderedDict((new_key, result[orig_key]) for orig_key, new_key in new_keys_map))

    return new_results


def order_search_results(results):
    # "d.desid, d.desname, d.desdate, projname, ra_pnt, dec_pnt, " \
    # "radepnt, o.keckid, o.firstnm, o.lastnm, o.email, o.institution"
    new_keys_map = [
        ('desid', 'Design-ID'), ('desname', 'Design-Name'), ('projname', 'Project-Name'),
        ('ra_pnt', 'RA'), ('dec_pnt', 'Declination'), ('radepnt', 'System'),
        ('keckid', 'Keck-ID'), ('firstnm', 'First-Name'), ('lastnm', 'Last-Name'),
        ('email', 'Email'), ('institution', 'Institution'), ('desdate', 'Design-Date')
    ]

    new_results = []
    for result in results:
        new_results.append(OrderedDict((new_key, result[orig_key]) for orig_key, new_key in new_keys_map))

    return new_results


def get_keck_obs_info(url_params, obs_info_url):
    """
    Performs a an API query
    """
    log = log_fun.get_log()

    url = f"{obs_info_url['info_url']}?{url_params}"

    # Make a GET request to the API endpoint
    response = requests.get(url, verify=False)

    try:
        observer_dict = response.json()
        if not observer_dict:
            log.warning(f'no observer found for {url_params} {observer_dict}')
            return None
    except Exception as err:
        print(f'error accessing url: {url}')

    return observer_dict
