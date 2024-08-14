import sys
from os import path

from datetime import datetime
import email_utils as utils

APP_PATH = path.abspath(path.dirname(__file__))

if __name__ == '__main__':

    cfg, log = utils.start_up(APP_PATH)
    log.info('-- PI Notification Email script for newly milled masks --')

    today = datetime.utcnow()
    mail_subject = f"Keck Slitmasks have been milled {today.strftime('%Y-%m-%d')}"

    mail_server = utils.get_cfg(cfg, 'email', 'server')
    mail_from = utils.get_cfg(cfg, 'email', 'from')

    api_url = utils.get_cfg(cfg, 'slitmask_api', 'api_url')
    recently_scanned = utils.get_cfg(cfg, 'slitmask_api', 'recent_scans_emails')

    full_url = f"{api_url}/{recently_scanned}"

    json_output = utils.query_db_api(full_url)

    try:
        if not json_output['data']:
            log.info('no masks have been recently milled,  not sending PI emails.')
            sys.exit()
    except Exception as e:
        log.error(f'No JSON returned API might be down. ERROR: {e}')
        sys.exit()

    mail_header = """
    <html lang="en">
    <head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <style>
        table {
            border-collapse: collapse;
            width: 100%;
            max-width: 600px;
        }
        th, td {
            border: 1px solid white;
            padding: 8px;
            text-align: left;
        }
        .content {
            margin-bottom: 40px; 
        }
    </style>
    
    </head>
    <body>
    <div class="content">
        <p>Automated notification from the W.M. Keck Observatory Slitmask 
        milling facility.</p>
        <p>Within the past 2 days the following masks have been milled:</p>
    """

    mail_footer = """
       </div>

        <div style="margin-bottom: 40px;">&nbsp;</div>
        </body>
        </html>
        """

    # loop through the results sending an email to each PI
    for mail_to, entries in json_output["data"].items():
        html_table = utils.json_to_html_table({'data': entries})
        mail_msg = mail_header + html_table + mail_footer

        log.info(f"Sending PI Newly Milled Mask Email to: {mail_to}")

        # TODO remove when ready
        mail_to = 'lfuhrman@keck.hawaii.edu'
        utils.send_email(mail_to, mail_from, mail_msg, mail_subject, mail_server)








