# Copyright (C) 2025 Entidad Pública Empresarial Red.es
#
# This file is part of "dge-ga-report (datos.gob.es)".
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

import os
import httplib2
from apiclient.discovery import build
from oauth2client.client import flow_from_clientsecrets
from oauth2client.file import Storage
from oauth2client import tools
from oauth2client.service_account import ServiceAccountCredentials
import logging
log = logging.getLogger(__name__)


from ckan.plugins.toolkit import (config)


def _prepare_credentials(credentials_file):
    """
    Either returns the user's oauth credentials or uses the credentials
    file to generate a token (by forcing the user to login in the browser)
    """
    scope = ['https://www.googleapis.com/auth/analytics.readonly']
    credentials = ServiceAccountCredentials.from_json_keyfile_name(
        credentials_file,
        scopes=scope
    )
    return credentials


def init_service(credentials_file, is_ga4=False):
    """
    Given a file containing the user's oauth token (and another with
    credentials in case we need to generate the token) will return a
    service object representing the analytics API.
    """
    credentials = _prepare_credentials(credentials_file)

    if is_ga4:
        return build('analyticsdata', 'v1beta', credentials=credentials, cache_discovery=False)
    else:
        http = httplib2.Http()
        http = credentials.authorize(http)
        return build('analytics', 'v3', http=http, cache_discovery=False)


def get_profile_id(service, webPropertyId, view_id):
    """
    Get the profile ID for this user and the service specified by the
    'googleanalytics.id' configuration option. This function iterates
    over all of the accounts available to the user who invoked the
    service to find one where the account name matches (in case the
    user has several).
    """
    accounts = service.management().accounts().list().execute()

    if not accounts.get('items'):
        return None

    accountName = config.get('googleanalytics.account')
    for acc in accounts.get('items'):
        if acc.get('name') == accountName:
            accountId = acc.get('id')

    webproperties = service.management().webproperties().list(accountId=accountId).execute()

    profiles = service.management().profiles().list(
        accountId=accountId, webPropertyId=webPropertyId).execute()

    if profiles.get('items'):

        if view_id:
            for item in profiles.get('items'):
                if item and item.get('id') and item.get('id') == view_id:
                    return item.get('id')
        else:
            return profiles.get('items')[0].get('id')

    return None
