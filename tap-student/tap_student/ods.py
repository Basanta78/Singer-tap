import requests
from urllib.parse import urlencode
import json


class ODSConnection:
    def __init__(self, provider, client_id, client_secret, api_route='/api/v2.0/2018'):
        self.provider = provider
        self.hostname = provider + api_route
        self.authorize(client_id, client_secret)

    def authorize(self, client_id, client_secret):
        querystring_data = {
            'Client_id': client_id,
            'Response_type': "code",
        }
        auth_url = '%s/oauth/authorize?%s' % (self.provider, urlencode(querystring_data))
        r = requests.get(auth_url)
        web_api_key = json.loads(r.content)["code"]

        querystring_data = {
            'Client_id': client_id,
            'Client_secret': client_secret,
            'Grant_type': "authorization_code",
            'Code': web_api_key,
        }
        token_url = '%s/oauth/token?%s' % (self.provider, urlencode(querystring_data))
        r = requests.get(token_url)
        self.access_token = json.loads(r.content)["access_token"]

    def connect_domain(self, domain):
        return APIWrapper(self.hostname, self.access_token, domain)


class APIWrapper:
    def __init__(self, hostname, access_token, domain):
        self.hostname = hostname
        self.headers = {
            'Authorization': 'Bearer %s' % access_token,
        }
        self.domain = domain

    def get_all(self, **kwargs):
        url = '%s/%s?%s' % (self.hostname, self.domain, urlencode(kwargs))
        r = requests.get(url, headers=self.headers)
        return r.status_code, json.loads(r.content)

    def get_by_id(self, domain_id):
        url = '%s/%s/%s' % (self.hostname, self.domain, domain_id)
        r = requests.get(url, headers=self.headers)
        return json.loads(r.content)

    def delete_by_id(self, domain_id):
        url = '%s/%s/%s' % (self.hostname, self.domain, domain_id)
        r = requests.delete(url, headers=self.headers)
        return json.loads(r.content)

    def post(self, data):
        if 'id' in data.keys(): del data['id']

        url = '%s/%s' % (self.hostname, self.domain)
        headers = {
            **self.headers,
            'Content-Type': 'application/json',
        }
        r = requests.post(url, headers=headers, data=json.dumps(data))

        return r.status_code, r.content
