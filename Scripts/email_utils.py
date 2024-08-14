import configparser
import requests
import logging
import sys

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate

SLITMASK_LOGNAME = 'slitmask_emails'

import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def start_up(app_path, config_name='slitmask_emails.ini'):
    """
    The API start up configuration.

    :param app_path: <str> the path that the api is running
    :param config_name: <str> name of the config file.

    :return: config as an object, and the initialized log.
    """
    config_filename = config_name
    config_file = f'{app_path}/{config_filename}'
    config = configparser.ConfigParser()
    config.read(config_file)

    try:
        log_dir = get_cfg(config, 'general', 'log_dir')
    except KeyError:
        log_dir = f'{app_path}/log'

    log = configure_logger(log_dir)

    return config, log


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


def send_email(mail_to, mail_from, mail_msg, mail_subject, mail_server, log=None):
    """
    send an email if there are any warnings or errors logged.

    :param mail_msg: <str> message to mail.
    :param config: <class 'configparser.ConfigParser'> the config file parser.
    """
    if not mail_msg:
        return

    msg = MIMEMultipart()

    msg['To'] = mail_to
    msg['From'] = mail_from
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = mail_subject

    msg.attach(MIMEText(mail_msg, 'html'))

    try:
        server = smtplib.SMTP(mail_server)
        server.sendmail(mail_from, mail_to, msg.as_string())
        server.quit()
    except Exception as err:
        if log:
            log.warning(f"Error sending Email. Error: {err}.")
        else:
            print(f"Error sending Email. Error: {err}.")


def query_db_api(url):
    try:
        response = requests.get(url, verify=False)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        return f"Error fetching data: {e}"


def json_to_html_table(json_data):
    html = '<table border="1" cellpadding="5" cellspacing="0">\n'

    # table header
    headers = json_data['data'][0].keys()
    html += '  <tr>\n'
    for header in headers:
        html += f'    <th>{header}</th>\n'
    html += '  </tr>\n'

    # table rows
    for entry in json_data['data']:
        html += '  <tr>\n'
        for header in headers:
            html += f'    <td>{entry[header]}</td>\n'
        html += '  </tr>\n'

    html += '</table>'
    return html


def configure_logger(log_dir):
    """
    log_dir <str>: the path to the log
    """
    log_name = SLITMASK_LOGNAME

    # get the log if already exists
    log = logging.getLogger(log_name)
    if log.handlers:
        return log

    # set-up the logger
    log_path = f'{log_dir}/{log_name}.log'
    log.setLevel(logging.INFO)

    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(funcName)s - %(message)s')

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(formatter)
    log.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    log.addHandler(stream_handler)

    return log
