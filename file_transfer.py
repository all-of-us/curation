from base64 import b64encode
from io import BytesIO
from uuid import uuid4
import requests
import time
import wddx

import settings


TRANSFER_API_URL_FMT = 'https://transfer.nyp.org/seos/1000/%s.api'
TRANSFER_LOGIN_URL = TRANSFER_API_URL_FMT % 'login'
TRANSFER_FIND_URL = TRANSFER_API_URL_FMT % 'find'
TRANSFER_PUT_URL = TRANSFER_API_URL_FMT % 'put'
TRANSFER_SEND_URL = TRANSFER_API_URL_FMT % 'send'
SEND_DELAY_SECONDS = 1.5  # Accellion recommends 5 seconds, ain't nobody got time for that
UPLOAD_TIMEOUT_SECONDS = 60 * 2


def get_tokens():
    """
    Retrieve Accellion API tokens
    :return:
    """
    data = {'auth_type': 'pwd',
            'uid': settings.accellion['username'],
            'pwd': settings.accellion['password'],
            'api_token': 1,
            'output': 'json'}
    response = requests.post(TRANSFER_LOGIN_URL, data=data)
    return response.json()


def parse_response(content):
    items = wddx.loads(content)
    return items[0]


def upload(filename, file_contents, recipients, mime_type='text/plain', subject=None, message=None, expire_days=21):
    """
    Upload a file to the Accellion file system

    :param filename: user-friendly filename
    :param file_contents: binary data; this supports streamed data to prevent reading into memory
    :param recipients: comma-separated list of e-mail addresses
    :param subject: subject of e-mail
    :param message: body of e-mail
    :param mime_type: type of file
    :param expire_days: number of days until link expires
    :return: details from put and send api calls
    """

    tokens = get_tokens()
    uid = uuid4().__str__()
    file_handle = '%s/files/%s/%s' % (tokens['client_id'], uid, filename)
    data = {'token':  tokens['put_token'], 'file_handle': file_handle}
    put_response = requests.post(TRANSFER_PUT_URL, data=data, files={'file': (filename, file_contents, mime_type)})
    put_details = parse_response(put_response.content)

    # create e-mail package with links to file (short-url mode)
    time.sleep(SEND_DELAY_SECONDS)
    meta_file_handle = '%s/files/%s-list' % (tokens['client_id'], uid)
    file_handle = put_details['file_handle']
    file_size = put_details['file_size']
    file_handle_hash = b64encode(file_handle)
    file_list = '%s\n|%s\n|%s\n|\n' % (b64encode(filename), file_handle_hash, b64encode(file_size))
    data = {'token': tokens['send_token'],
            'short_token': 1,
            'sender': b64encode(settings.accellion['username']),
            'recipients': b64encode(recipients),
            'meta_file_handle': meta_file_handle,
            'file_list1': file_list,
            'link_validity': expire_days,
            'email_options1': 'vr'}  # only allow the original recipient to download
    if subject is not None:
        data['subject'] = b64encode(subject)
    if message is not None:
        data['message'] = b64encode(message)
    send_response = requests.post(TRANSFER_SEND_URL, data=data, timeout=UPLOAD_TIMEOUT_SECONDS)
    send_details = parse_response(send_response.content)
    response_details = dict(put=put_details, send=send_details)
    return response_details


def download(url, dest):
    """
    Download file from secure file transfer and save it to specified location
    :param url: url of file in secure file transfer
    :param dest: path to save file to
    :return:
    """
    tokens = get_tokens()
    cookie_value = 'user&%s&cs&$%s' % (settings.accellion['username'], tokens['inbox_cs'])
    r = requests.get(url, cookies=dict(a1000c1s1=cookie_value))
    bs = BytesIO(r.content)
    with open(dest, 'wb') as out:
        out.write(bs.read())


def inbox():
    """
    Retrieve list of e-mail packages
    :return:
    """
    tokens = get_tokens()
    data = dict(token=tokens['inbox_token'], mailbody=1)
    inbox_response = requests.post(TRANSFER_FIND_URL, data=data)
    return parse_response(inbox_response.content)
