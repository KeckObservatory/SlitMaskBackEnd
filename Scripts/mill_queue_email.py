from os import path
import sys

from datetime import datetime
import email_utils as utils

from collections import defaultdict


CONFIG_FILE = "slitmask_emails.live.ini"
APP_PATH = path.abspath(path.dirname(__file__))


def create_work_table(json_data):
    result_by_date = defaultdict(lambda: {"num_masks": 0, "total_slits": 0})

    for entry in json_data["data"]:
        use_date = datetime.strptime(entry["Use-Date"], "%Y-%m-%d").date()
        result_by_date[use_date]["num_masks"] += 1
        result_by_date[use_date]["total_slits"] += entry["Number-Slits"]

    final_result = {
        str(date): {"masks": data["num_masks"], "slits": data["total_slits"]}
        for date, data in result_by_date.items()
    }

    html_output = """
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
        .row {
            height: 30px; 
        }
        .spaced-table {
            margin-bottom: 30px;  
        }
    </style>
    <table class="spaced-table">
        <thead>
            <tr>
                <th>Use Date</th>
                <th>Number of Masks</th>
                <th>Number of Slits</th>
            </tr>
        </thead>
        <tbody>
    """

    row_count = 0

    for index, (date, data) in enumerate(final_result.items()):
        html_output += f"<tr class='row'><td>{date}</td><td>" \
                       f"{data['masks']}</td><td>{data['slits']}</td></tr>"
        row_count += 1

    html_output += "</tbody></table>"

    return html_output


if __name__ == '__main__':

    cfg, log = utils.start_up(APP_PATH)
    log.info('-- Masks Recently Milled Notification Email script --')

    today = datetime.utcnow()
    mail_subject = f"Current Masks in Slitmask Queue {today.strftime('%Y-%m-%d')}"

    mail_server = utils.get_cfg(cfg, 'email', 'server')
    mail_to = utils.get_cfg(cfg, 'email', 'info')
    mail_from = utils.get_cfg(cfg, 'email', 'from')

    api_url = utils.get_cfg(cfg, 'slitmask_api', 'api_url')
    mill_queue = utils.get_cfg(cfg, 'slitmask_api', 'mill_queue')

    full_url = f"{api_url}/{mill_queue}"

    json_output = utils.query_db_api(full_url)
    try:
        if not json_output['data']:
            log.info('no masks have been recently milled,  not sending an email')
            sys.exit()
    except Exception as e:
        log.error(f'No JSON returned API might be down. ERROR: {e}')
        sys.exit()

    html_table = utils.json_to_html_table(json_output)

    mail_msg = create_work_table(json_output)
    mail_msg += html_table

    log.info(f'Sending Mill Queue Mask Notification Email to: {mail_to}.')

    utils.send_email(mail_to, mail_from, mail_msg, mail_subject, mail_server)









