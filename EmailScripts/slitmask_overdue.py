import sys
from os import path

from datetime import datetime
import email_utils as utils
import configparser

CONFIG_FILE = "slitmask_emails.ini"
APP_PATH = path.abspath(path.dirname(__file__))

if __name__ == '__main__':

    cfg, log = utils.start_up(APP_PATH)
    log.info('-- Mask Overdue Notification Email script --')

    today = datetime.utcnow()
    mail_subject = f"Overdue Masks {today.strftime('%Y-%m-%d')}"

    mail_server = utils.get_cfg(cfg, 'email', 'server')
    # mail_to = utils.get_cfg(cfg, 'email', 'alarm')
    mail_from = utils.get_cfg(cfg, 'email', 'from')

    api_url = utils.get_cfg(cfg, 'slitmask_api', 'api_url')
    overdue_masks = utils.get_cfg(cfg, 'slitmask_api', 'overdue_masks')

    full_url = f"{api_url}/{overdue_masks}"

    json_output = utils.query_db_api(full_url)
    try:
        if not json_output['data']:
            log.info('no masks are overdue,  not sending an email')
            sys.exit()
    except Exception as e:
        log.error(f'No JSON returned API might be down. ERROR: {e}')
        sys.exit()

    html_table = utils.json_to_html_table(json_output)

    # TODO remove when ready
    mail_to = 'lfuhrman@keck.hawaii.edu'

    mail_msg = html_table

    log.info(f'Sending Mask Overdue Notification Email to: {mail_to}.')

    utils.send_email(mail_to, mail_from, mail_msg, mail_subject, mail_server)








