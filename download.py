import requests
import xmltodict
from sqlalchemy import BigInteger, String, Column, MetaData, Table
from run_config import engine

import settings

TRANSFER_API_URL_FMT = 'https://transfer.nyp.org/seos/1000/%s.api'
TRANSFER_LOGIN_URL = TRANSFER_API_URL_FMT % 'login'
TRANSFER_FIND_URL = TRANSFER_API_URL_FMT % 'find'


def get_tokens():
    """
    Logs into Accellion and retrieves API tokens
    :return:
    """
    data = {'auth_type': 'pwd',
            'uid': settings.accellion['username'],
            'pwd': settings.accellion['password'],
            'api_token': 1,
            'output': 'json'}
    response = requests.post(TRANSFER_LOGIN_URL, data=data)
    return response.json()


def parse_struct(d):
    if type(d) is list:
        result = []
        for item in d:
            result.append(parse_struct(item))
        return result
    else:
        result = dict()
        for item in d['var']:
            key = item['@name']
            result[key] = parse_value(item)
        return result


def parse_value(d):
    if 'string' in d:
        return d['string']
    if 'null' in d:
        return None
    if 'array' in d:
        array = d['array']
        if array['@length'] in ['0', '1']:
            return [parse_value(array)]
        else:
            return parse_value(array)
    if 'struct' in d:
        return parse_struct(d['struct'])


def list_files():
    metadata = MetaData()
    table = Table('pmi_sprint_download',
                  metadata,
                  Column('sender_name', String(200), nullable=False),
                  Column('sent_time', BigInteger, nullable=False),
                  Column('file_handle', String(500), nullable=False),
                  Column('file_name', String(200), nullable=False),
                  Column('file_size', BigInteger, nullable=False),
                  Column('url', String(500), nullable=False))
    metadata.create_all(engine)
    tokens = get_tokens()
    data = dict(token=tokens['inbox_token'])
    inbox_response = requests.post(TRANSFER_FIND_URL, data=data)
    inbox_dict = xmltodict.parse(inbox_response.content)
    payload = parse_struct(inbox_dict['wddxPacket']['data']['struct'])
    for uid, package in payload['packages'].items():
        sender_name = package['sender_name']
        sent_time = int(package['sent_time'])
        for package_file in package['package_files']:
            file_handle = package_file['file_handle']
            file_name = file_handle.split('/')[-1]
            file_size = int(package_file['file_size'])
            url = package_file['url']
            query = table.select().where(table.c.file_handle == file_handle)
            results = engine.execute(query).fetchall()
            if len(results) == 0:
                # TODO download the file, store on queue
                engine.execute(table.insert(),
                               sender_name=sender_name,
                               sent_time=int(sent_time),
                               file_handle=file_handle,
                               file_name=file_name,
                               file_size=int(file_size),
                               url=url)


if __name__ == '__main__':
    list_files()
