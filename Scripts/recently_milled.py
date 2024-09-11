import sys
from os import path

from datetime import datetime
import email_utils as utils
import configparser

CONFIG_FILE = "slitmask_emails.ini"
APP_PATH = path.abspath(path.dirname(__file__))

if __name__ == '__main__':

    cfg, log = utils.start_up(APP_PATH)
    log.info('-- Masks Recently Milled Notification Email script --')

    today = datetime.utcnow()
    mail_subject = f"Recently Milled Masks {today.strftime('%Y-%m-%d')}"

    mail_server = utils.get_cfg(cfg, 'email', 'server')
    mail_to = utils.get_cfg(cfg, 'email', 'info')
    mail_from = utils.get_cfg(cfg, 'email', 'from')

    api_url = utils.get_cfg(cfg, 'slitmask_api', 'api_url')
    recently_scanned = utils.get_cfg(cfg, 'slitmask_api', 'recent_scans')

    full_url = f"{api_url}/{recently_scanned}"

    json_output = utils.query_db_api(full_url)

    try:
        if not json_output['data']:
            log.info('no masks have been recently milled,  not sending an email')
            sys.exit()
    except Exception as e:
        log.error(f'No JSON returned API might be down. ERROR: {e}')
        sys.exit()

    # remove the time stamp field,  add the number of days until mask is used
    for mask_info in json_output['data']:
        if 'millid' in mask_info:
            del mask_info['millid']

    html_table = utils.json_to_html_table(json_output)

    mail_msg = html_table

    log.info(f'Sending Recently Milled Mask Notification Email to: {mail_to}.')

    utils.send_email(mail_to, mail_from, mail_msg, mail_subject, mail_server)








