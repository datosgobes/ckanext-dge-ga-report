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

import logging
import ckan.lib.helpers as h
import ckan.plugins as p
from ckan.plugins import toolkit

import ckanext.dge_ga_report.cli as cli

log = logging.getLogger('ckanext.dge_ga_report')

class DgeGAReportException(Exception):
    pass


class DgeGaReportPlugin(p.SingletonPlugin):
    p.implements(p.IConfigurer, inherit=True)
    p.implements(p.IClick, inherit=True)

    def get_commands(self):
        return cli.get_commands()

    def update_config(self, config):
        toolkit.add_template_directory(config, 'templates')
        toolkit.add_public_directory(config, 'public')
