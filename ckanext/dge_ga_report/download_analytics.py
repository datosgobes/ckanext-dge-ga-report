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
import datetime
import collections
import requests
import time
import re
import logging
import urllib.request, urllib.parse, urllib.error

from ckan.plugins.toolkit import (config)
from . import ga_model

log = logging.getLogger(__name__)

FORMAT_MONTH = '%Y-%m'
MIN_VIEWS = 50
MIN_VISITS = 20



class DownloadAnalytics(object):
    '''Downloads and stores analytics info'''

    KIND_STAT_PACKAGE_RESOURCES = 'pages'
    KIND_STAT_VISITS = 'sessions'
    KIND_STATS = [KIND_STAT_PACKAGE_RESOURCES, KIND_STAT_VISITS]

    PACKAGE_STAT = 'dge_ga_package'
    RESOURCE_STAT = 'dge_ga_resource'
    VISIT_STAT = 'dge_ga_visit'

    URL_PREFIX = '^(|/es|/en|/eu|/ca|/gl)/'
    URL_SUFFIX = '[/?].+'

    NAME_REGEX = '[a-z0-9-_]+'
    PACKAGE_URL_REGEX = URL_PREFIX + 'catalogo/' + NAME_REGEX + '/?$'
    PACKAGE_SECCIONS2_REGEX = '^(conjuntos de datos|datasets|conjunts de dades|conxuntos de datos|datu-multzoak|servicios de datos|dataservices|serveis de dades|servizos de datos|datu-zerbitzuak)$'
    PACKAGE_SECCIONS2_REGEX_UA = PACKAGE_SECCIONS2_REGEX + ';ga:dimension4!=(not set)'
    PACKAGE_URL_EXCLUDED_REGEXS = [
       
    ]
    ID_REGEX = '[a-z0-9-]+'
    
    RESOURCE_URL_REGEX = PACKAGE_URL_REGEX
    RESOURCE_SECCIONS2_REGEX = PACKAGE_SECCIONS2_REGEX
    RESOURCE_URL_EXCLUDED_REGEXS = [
        URL_PREFIX + 'catalogo/new/?$'
    ]

    CATALOG_URL_EXCLUDED_REGEXS = [
        URL_PREFIX + 'catalogo/new(/?|\?.*)$',
        URL_PREFIX + 'catalogo/(edit|resources|new_resource)/' + NAME_REGEX + '(|/|/.+)$',
        URL_PREFIX + 'catalogo/' + NAME_REGEX + '/(resource_edit|resource)/' + ID_REGEX + '(|/|/.+)$'
    ]

    SECTIONS_GTM = [
        {
            'key': 'all',
            'name': '',
            'url_regex': '',
            'exluded_url_regex': [],
            'metrics': 'ga:sessions',
            'sort': '-ga:sessions'
        },
        {
            'key': 'section',
            'name': 'catalogo',
            'seccions2_regex': PACKAGE_SECCIONS2_REGEX,
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'iniciativas',
            'seccions2_regex': '^(mapa de iniciativas|initiative map|mapa d\'iniciatives|ekimenen mapa)$',
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'documentacion',
            'seccions2_regex': '^(documentacion|documentation|documentacio|dokumentazioa)$',
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'aplicaciones',
            'seccions2_regex': '^(aplicaciones|applications|aplicacions|aplikazioak)$',
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews',
            'isprueba': True
        },
        {
            'key': 'section',
            'name': 'empresas-reutilizadoras',
            'seccions2_regex': '^(empresas reutilizadoras|reuse companies|empreses reutilitzadores|enpresa berrerabiltzaileak)$',
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'peticiones-datos',
            'seccions2_regex': '^(disponibilidad de datos|data availability|disponibilitat de dades|disponibilidade de datos|datuen erabilgarritasuna)$',
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'dashboard',
            'seccions2_regex': '^(cuadro de mando|dashboard|quadre de comandament|cadro de mando|aginte-koadroa)$',
            'exluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'noticias',
            'seccions2_regex': '^(noticias|news|noticies|berriak)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'eventos',
            'seccions2_regex': '^(eventos|events|esdeveniments|gertaerak|jardunaldiak)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'entrevistas',
            'seccions2_regex': '^(entrevistas|interviews|entrevistes|elkarrizketak)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'boletines',
            'seccions2_regex': '^(boletines|newsletters|butlletins|boletins|buletinak)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'blog_blog',
            'seccions2_regex': '^(blog|bloc|bloga|blog-a)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'agricultura',
            'seccions2_regex': '^(agricultura|environment|medi ambient|medio ambiente|ingurumena)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'cultura',
            'seccions2_regex': '^(cultura y ocio|culture and leisure|cultura i lleure|cultura e lecer|kultura eta aisia)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'educacion',
            'seccions2_regex': '^(educacion|education|educacio|hezkuntza)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'transporte',
            'seccions2_regex': '^(transporte|transport|garraioa)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'salud-bienestar',
            'seccions2_regex': '^(salud y bienestar|health & wellness|salut i benestar|saude e benestar)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'turismo',
            'seccions2_regex': '^(turismo|tourism|turisme|turismoa)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        },
        {
            'key': 'section',
            'name': 'justicia-sociedad',
            'seccions2_regex': '^(justicia y sociedad|justice and society|justicia i societat|xustiza e sociedade|justizia eta gizartea)$',
            'excluded_url_regex': [],
            'metrics': 'ga:pageviews',
            'sort': '-ga:pageviews'
        }
    ]

    SECTIONS_GTM_GA4 = [
        {
            'key': 'all',
            'name': '',
            'url_regex': '',
            'exluded_url_regex': [],
            'metrics': 'sessions',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'catalogo',
            'seccions2_regex': PACKAGE_SECCIONS2_REGEX,
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'iniciativas',
            'seccions2_regex': '^(iniciativas|initiatives|iniciatives|ekimenen|mapa de iniciativas|initiative map|mapa d\'iniciatives|ekimenen mapa)$',
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'documentacion',
            'seccions2_regex': '^(documentacion|documentation|documentacio|dokumentazioa)$',
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'aplicaciones',
            'seccions2_regex': '^(aplicaciones|applications|aplicacions|aplikazioak)$',
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True,
            'isprueba': True
        },
        {
            'key': 'section',
            'name': 'empresas-reutilizadoras',
            'seccions2_regex': '^(empresas|companies|empreses|enpresak|empresas reutilizadoras|reuse companies|empreses reutilitzadores|enpresa berrerabiltzaileak)$',
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'peticiones-datos',
            'seccions2_regex': '^(solicitud de datos|data request|sol·licitud de dades|solicitude de datos|datuak eskatzea|disponibilidad de datos|data availability|disponibilitat de dades|disponibilidade de datos|datuen erabilgarritasuna)$',
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'dashboard',
            'seccions2_regex': '^(metricas e impacto|metrics and impact|metriques i impacte|metrikak eta eragina|cuadro de mando|dashboard|quadre de comandament|cadro de mando|aginte-koadroa)$',
            'exluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'noticias',
            'seccions2_regex': '^(noticias|news|noticies|novas|berriak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'eventos',
            'seccions2_regex': '^(eventos|events|esdeveniments|gertaerak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'entrevistas',
            'seccions2_regex': '^(entrevistas|interviews|entrevistes|elkarrizketak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'boletines',
            'seccions2_regex': '^(boletines|newsletters|butlletins|boletins|buletinak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'blog_blog',
            'seccions2_regex': '^(blog|bloc|bloga)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'apoyo-publicadores',
            'seccions2_regex': '^(apoyo a publicadores|support for publishers|recolzament a publicadors|apoio a publicadores|argitaratzaileentzako laguntza|asesoramiento y soporte|advice and support|assessorament i suport|asesoramento e soporte|aholkularitza eta laguntza)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'entornos-seguros',
            'seccions2_regex': '^(entornos seguros|safe environments|entorns segurs|interfaces seguras|ingurune seguruak|acceso nsip|nsip access|acces nsip|nsip sarbidea)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'espacios-datos',
            'seccions2_regex': '^(espacios de datos|data spaces|espais de dades|espazos de datos|datuen eremua)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'desafios',
            'seccions2_regex': '^(desafios|challenges|desafiaments|erronkak|desafio aporta|aporta challenge|desafiament aporta|aporta erronka)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'sectores',
            'seccions2_regex': '^(sectores|sectors|sektoreak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'ejercicios-datos',
            'seccions2_regex': '^(ejercicios de datos|data exercises|exercicis de dades|exercicios de datos|datuen erabilerak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'infografias',
            'seccions2_regex': '^(infografias|infographics|infografies|infografiak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'informes-guias',
            'seccions2_regex': '^(informes y guias|reports and guides|informes i guies|informes e guias|informeak eta gidak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'encuentros-aporta',
            'seccions2_regex': '^(encuentros aporta|aporta meetings|trobades aporta|encontros achega|aporta bilerak|encontros aporta)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'que-hacemos',
            'seccions2_regex': '^(que hacemos|what we do|que fem|que facemos|aporta ekimenari buruz|zer egiten dugu|acerca de la iniciativa aporta|about the aporta initiative|sobre la iniciativa aporta|sobre a iniciativa aporta|aporta ekimenari buruz)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'preguntas-frecuentes',
            'seccions2_regex': '^(preguntas frecuentes|frequently asked questions|preguntes frequents|ohiko galderak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'tecnologia',
            'seccions2_regex': '^(tecnologia|technology|tecnoloxia|teknologia)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'contacto',
            'seccions2_regex': '^(contacto|contact|contacte|kontaktua)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'mapa-sitio',
            'seccions2_regex': '^(mapa del sitio|site map|mapa del lloc|mapa do sitio|lekuaren mapa)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'api',
            'seccions2_regex': '^(api)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'punto-sparql',
            'seccions2_regex': '^(punto sparql|sparql endpoint|punt sparql|sparql puntua)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'accesibilidad',
            'seccion':'customEvent:seccion_s1',
            'seccions2_regex': '^(accesibilidad|accessibility|accessibilitat|accesibilidade|irisgarritasuna|eskuragarritasuna)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'aviso-legal',
            'seccion':'customEvent:seccion_s1',
            'seccions2_regex': '^(aviso legal|legal notice|avis legal|legezko abisu)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'agricultura',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(agricultura|environment|medi ambient|medio ambiente|ingurumena)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'cultura',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(cultura y ocio|culture and leisure|cultura i lleure|cultura e lecer|kultura eta aisia)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'educacion',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(educacion|education|educacio|hezkuntza)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'transporte',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(transporte|transport|garraioa)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'salud-bienestar',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(salud|healthcare|salut|saude|osasuna|salud y bienestar|health & wellness|salut i benestar|saude e benestar)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'turismo',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(turismo|tourism|turisme|turismoa)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'justicia-sociedad',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(legislacion y justicia|legislation and justice|legislacio i justicia|lexislacion e xustiza|legegintza eta justizia|justicia y sociedad|justice and society|justicia i societat|xustiza e sociedade|justizia eta gizartea)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'ciencia-tecnologia',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(ciencia y tecnologia|science and technology|ciencia i tecnologia|ciencia e tecnoloxia|zientzia eta teknologia)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'comercio',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(comercio|commerce|comerç|merkataritza)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'demografia',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(demografia|demography)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'deporte',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(deporte|sport|esport|kirola)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'economia',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(economia|economy|ekonomia)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'empleo',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(empleo|employment|ocupacio|emprego|enplegua)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'energia',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(energia|energy|enerxia)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'hacienda',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(hacienda|treasury|hisenda|facenda|ogasuna)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'industria',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(industria|industry)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'medio-rural',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(medio rural|rural environment|medi rural|nekazaritza)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'sector-publico',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(sector publico|public sector|sector public|sektore publikoa)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'seguridad',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(seguridad|security|seguretat|seguridade|segurtasuna)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'sociedad-bienestar',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(sociedad y bienestar|society and welfare|societat i benestar|sociedade e benestar|gizartea eta ongizatea)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'urbanismo-infraestructuras',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(urbanismo e infraestructuras|town planning and infrastructures|urbanisme i infraestructures|urbanismo e infraestruturas|hirigintza eta azpiegiturak)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        },
        {
            'key': 'section',
            'name': 'vivienda',
            'seccion':'customEvent:seccion_s3',
            'seccions2_regex': '^(vivienda|housing|habitatge|vivenda|etxebizitza)$',
            'excluded_url_regex': [],
            'metrics': 'eventCount',
            'sort': True
        }
    ]

    def __init__(self, service=None, token=None, profile_id=None, profile_id_gtm=None,
                 delete_first=False, stat=None, print_progress=False,
                 kind_stats=None, save_stats=False, is_ga4=False):
        self.period = config.get('ckanext-dge-ga-report.period', 'monthly')
        self.hostname = config.get('ckanext-dge-ga-report.hostname', None)
        self.segment = config.get('ckanext-dge-ga-report.segment', None)
        self.default_filter = config.get('ckanext-dge-ga-report.filter', None)
        self.default_is_filter = config.get('ckanext-dge-ga-report.filter.is_filter', False)
        self.default_filter_is_excluded = config.get('ckanext-dge-ga-report.filter.is_excluded', False)
        self.default_filter_fieldname = config.get('ckanext-dge-ga-report.filter.fieldname', None)
        self.default_filter_matchtype = config.get('ckanext-dge-ga-report.filter.matchtype', None)
        self.default_filter_value = config.get('ckanext-dge-ga-report.filter.value', None)
        self.service = service
        self.profile_id = profile_id
        self.profile_id_gtm = profile_id_gtm
        self.delete_first = delete_first
        self.stat = stat
        self.token = token
        self.print_progress = print_progress
        self.kind_stats = kind_stats
        self.save_stats = save_stats
        self.is_ga4 = is_ga4
        self.property_id = 'properties/' + config.get('ckanext-dge-ga-report.view_id_ga4', None)
        self.property_id_gtm = 'properties/' + config.get('ckanext-dge-ga-report.view_id_ga4_gtm', None)

    def specific_month(self, date):
        import calendar

        first_of_this_month = datetime.datetime(date.year, date.month, 1)
        _, last_day_of_month = calendar.monthrange(int(date.year), int(date.month))
        last_of_this_month = datetime.datetime(date.year, date.month, last_day_of_month)
        # if this is the latest month, note that it is only up until today
        now = datetime.datetime.now()
        if now.year == date.year and now.month == date.month:
            last_day_of_month = now.day
            last_of_this_month = now
        periods = ((date.strftime(FORMAT_MONTH),
                    last_day_of_month,
                    first_of_this_month, last_of_this_month),)
        self.download_and_store(periods)

    def latest(self):
        if self.period == 'monthly':
            # from first of this month to today
            now = datetime.datetime.now()
            first_of_this_month = datetime.datetime(now.year, now.month, 1)
            periods = ((now.strftime(FORMAT_MONTH),
                        now.day,
                        first_of_this_month, now),)
        else:
            raise NotImplementedError
        self.download_and_store(periods)

    @staticmethod
    def get_full_period_name(period_name, period_complete_day):
        if period_complete_day:
            return period_name + ' (up to %ith)' % period_complete_day
        else:
            return period_name

    def download_and_store(self, periods):
        for period_name, period_complete_day, start_date, end_date in periods:
            log.info('Period "%s" (%s - %s)',
                     self.get_full_period_name(period_name, period_complete_day),
                     start_date.strftime('%Y-%m-%d'),
                     end_date.strftime('%Y-%m-%d'))
            print('period_name=%s' % period_name)
            if self.save_stats and self.delete_first:
                log.info('Deleting existing Analytics for this period "%s"',
                         period_name)
                ga_model.delete(period_name)

            if self.stat in (None, DownloadAnalytics.PACKAGE_STAT) and \
               self.kind_stats == DownloadAnalytics.KIND_STAT_PACKAGE_RESOURCES:
                # Clean out old dge_ga_package data before storing the new
                stat = DownloadAnalytics.PACKAGE_STAT
                if self.save_stats:
                    ga_model.pre_update_dge_ga_package_stats(period_name)
                log.info('Downloading analytics for package views')
                if self.is_ga4:
                    data = self.download(start_date, end_date,
                                         DownloadAnalytics.PACKAGE_SECCIONS2_REGEX,
                                         DownloadAnalytics.PACKAGE_URL_EXCLUDED_REGEXS,
                                         stat)
                else:
                    data = self.download(start_date, end_date,
                                         DownloadAnalytics.PACKAGE_SECCIONS2_REGEX_UA,
                                         DownloadAnalytics.PACKAGE_URL_EXCLUDED_REGEXS,
                                         stat)
                if data:
                    if self.save_stats:
                        log.info('Storing package views (%i rows)', len(data.get(stat, [])))
                        print('Storing package views (%i rows)' % (len(data.get(stat, []))))
                        self.store(period_name, period_complete_day, data, stat)
                        # Create the All records
                        ga_model.post_update_dge_ga_package_stats()
                    else:
                        print('The result contains %i rows:' % (len(data.get(stat, []))))
                        for row in data.get(stat):
                            print(row)

            if self.stat in (None, DownloadAnalytics.RESOURCE_STAT) and\
               self.kind_stats == DownloadAnalytics.KIND_STAT_PACKAGE_RESOURCES:
                # Clean out old dge_ga_package data before storing the new
                stat = DownloadAnalytics.RESOURCE_STAT
                if self.save_stats:
                    ga_model.pre_update_dge_ga_resource_stats(period_name)

                log.info('Downloading analytics for resource views')
                data = self.download(start_date, end_date,
                                     DownloadAnalytics.RESOURCE_URL_REGEX,
                                     DownloadAnalytics.RESOURCE_URL_EXCLUDED_REGEXS,
                                     stat)
                if data:
                    if self.save_stats:
                        log.info('Storing resource views (%i rows)', len(data.get(stat, [])))
                        print('Storing resource views (%i rows)' % (len(data.get(stat, []))))
                        self.store(period_name, period_complete_day, data, stat)
                        # Create the All records
                        ga_model.post_update_dge_ga_resource_stats()
                    else:
                        print('The result contains %i rows:' % (len(data.get(stat, []))))
                        for row in data.get(stat):
                            print(row)

            if self.stat in (None, DownloadAnalytics.VISIT_STAT) and \
               self.kind_stats == DownloadAnalytics.KIND_STAT_VISITS:
                # Clean out old dge_ga_package data before storing the new
                stat = DownloadAnalytics.VISIT_STAT
                if self.save_stats:
                    ga_model.pre_update_dge_ga_visit_stats(period_name)

                visits = []

                if self.is_ga4:
                    sections = DownloadAnalytics.SECTIONS_GTM_GA4
                else:
                    sections = DownloadAnalytics.SECTIONS_GTM

                for section in sections:
                    key = section.get('key', None)
                    name = section.get('name', None)
                    path = section.get('seccions2_regex', '')
                    path_section = section.get('seccion', 'customEvent:seccion_s2')
                    metrics = section.get('metrics', None)
                    sort = section.get('sort', None)
                    excluded_paths = section.get('exluded_url_regex', [])
                    if name or key:
                        print()
                        log.info(
                            'Downloading analytics %s for %s %s', metrics, name, key)
                        print('Downloading analytics %s for %s %s' % (metrics, name, key))
                        data = self.download(
                            start_date, end_date, path, excluded_paths, stat, path_section, metrics, sort)
                        if data:
                            visits.append((key, name, data.get(stat, 0)))
                if visits and len(visits) >= 1:
                    if self.save_stats:
                        log.info('Storing session visits (%i rows)', len(visits))
                        print('Storing session visits (%i rows)' % (len(visits)))
                        self.store(period_name, period_complete_day, {stat:visits}, stat)
                    else:
                        print('The result contains %i rows:' % (len(visits)))
                        for row in visits:
                            print(row)

    def download(self, start_date, end_date, path=None, exludedPaths=None, stat=None, path_section=None, metrics_stat=None, sort_stat='None'):
        '''Get views & visits data for particular paths & time period from GA
        '''
        if start_date and end_date and path is not None and stat:
            if stat not in [DownloadAnalytics.PACKAGE_STAT, DownloadAnalytics.RESOURCE_STAT, DownloadAnalytics.VISIT_STAT]:
                return {}
            start_date = start_date.strftime('%Y-%m-%d')
            end_date = end_date.strftime('%Y-%m-%d')
            print('Downloading analytics for stat %s, since %s, until %s with path %s' %(stat, start_date, end_date, path))

            if self.is_ga4:
                query = []
            else:
                query = None

            if stat == DownloadAnalytics.PACKAGE_STAT:
                if self.is_ga4:
                    if path:
                        query_filter = {
                            "filter": {
                                "fieldName": "eventName",
                                "stringFilter": {
                                    "matchType": "EXACT",
                                    "value": "load_complete",
                                    "caseSensitive": False
                                }
                            }
                        }
                        query.append(query_filter)
                        query_filter2 = {
                            "filter": {
                                "fieldName": 'customEvent:seccion_s2',
                                "stringFilter": {
                                    "matchType": "FULL_REGEXP",
                                    "value": path,
                                    "caseSensitive": False
                                }
                            }
                        }
                        query.append(query_filter2)
                        query_filter3 = {
                            "notExpression": {
                                "filter": {
                                    "fieldName": "customEvent:seccion_s3",
                                    "stringFilter": {
                                        "matchType": "FULL_REGEXP",
                                        "value": "^(|(not set))$",
                                        "caseSensitive": False
                                    }
                                }
                            }
                        }
                        query.append(query_filter3)
                    metrics = 'eventCount'
                    sort = True
                    dimensions = [{"name": "pagePath"}]
                else:
                    if path:
                        query = 'ga:dimension3=~%s' % path
                    metrics = 'ga:pageviews'
                    sort = '-ga:pageviews'
                    dimensions = "ga:dimension19"

            if stat == DownloadAnalytics.RESOURCE_STAT:
                if self.is_ga4:
                    query_filter = {
                        "filter": {
                            "fieldName": "customEvent:event_category",
                            "stringFilter": {
                                "matchType": "EXACT",
                                "value": "Resource",
                                "caseSensitive": False
                            }
                        }
                    }
                    query.append(query_filter)
                    if path:
                        path_filter = {
                            "filter": {
                                "fieldName": "pagePath",
                                "stringFilter": {
                                    "matchType": "FULL_REGEXP",
                                    "value": path,
                                    "caseSensitive": False
                                }
                            }
                        }
                        query.append(path_filter)
                    metrics = 'eventCount'
                    sort = True
                    dimensions = [
                        {
                            "name": "customEvent:event_label"
                        },
                        {
                            "name": "pagePath"
                        }
                    ]
                else:
                    query = 'ga:eventCategory==Resource;ga:eventAction==Download'
                    if path:
                        query += ';ga:pagePath=~%s' % path
                    metrics = 'ga:totalEvents'
                    sort = '-ga:totalEvents'
                    dimensions = "ga:eventLabel, ga:pagePath"
                if self.hostname:
                    if self.is_ga4:
                        query_filter = {
                            "filter": {
                                "fieldName": "hostName",
                                "stringFilter": {
                                    "matchType": "FULL_REGEXP",
                                    "value": self.hostname,
                                    "caseSensitive": False
                                }
                            }
                        }
                        query.append(query_filter)
                    else:
                        if query:
                            query += ';ga:hostname=~%s' % self.hostname
                        else:
                            query = 'ga:hostname=~%s' % self.hostname


            if stat == DownloadAnalytics.VISIT_STAT:

                if self.is_ga4:
                    if path and path_section:
                        query_filter = {
                            "filter": {
                                "fieldName": "eventName",
                                "stringFilter": {
                                    "matchType": "EXACT",
                                    "value": "load_complete",
                                    "caseSensitive": False
                                }
                            }
                        }
                        query.append(query_filter)
                        query_filter2 = {
                            "filter": {
                                "fieldName": path_section,
                                "stringFilter": {
                                    "matchType": "FULL_REGEXP",
                                    "value": path,
                                    "caseSensitive": False
                                }
                            }
                        }
                        query.append(query_filter2)
                    if metrics_stat:
                        metrics = metrics_stat
                    if sort_stat:
                        sort = sort_stat
                    dimensions = []
                else:
                    if path:
                        query = 'ga:dimension3=~%s' % path
                    if metrics_stat:
                        metrics = metrics_stat
                    if sort_stat:
                        sort = sort_stat
                    dimensions = ""

            if exludedPaths:
                for path in exludedPaths:
                    if self.is_ga4:
                        query_filter = {
                            "notExpression": {
                                "filter": {
                                    "fieldName": "pagePath",
                                    "stringFilter": {
                                        "matchType": "FULL_REGEXP",
                                        "value": path,
                                        "caseSensitive": False
                                    }
                                }
                            }
                        }
                        query.append(query_filter)
                    else:
                        if query:
                            query += ';ga:pagePath!~%s' % path
                        else:
                            query = 'ga:pagePath!~%s' % path

            if self.default_is_filter:
                if self.is_ga4:
                    if self.default_filter_is_excluded:
                        query_filter = {
                            "notExpression": {
                                "filter": {
                                    "fieldName": self.default_filter_fieldname,
                                    "stringFilter": {
                                        "matchType": self.default_filter_matchtype,
                                        "value": self.default_filter_value,
                                        "caseSensitive": False
                                    }
                                }
                            }
                        }
                    else:
                        query_filter = {
                            "filter": {
                                "fieldName": self.default_filter_fieldname,
                                "stringFilter": {
                                    "matchType": self.default_filter_matchtype,
                                    "value": self.default_filter_value,
                                    "caseSensitive": False
                                }
                            }
                        }
                    query.append(query_filter)
                else:
                    if query:
                        query += ';%s' % self.default_filter
                    else:
                        query += '%s' % self.default_filter

            # Supported query params at
            # https://developers.google.com/analytics/devguides/reporting/core/v3/reference
            try:
                args = {}
                args["sort"] = sort
                args["max-results"] = 100000
                args["dimensions"] = dimensions
                args["start-date"] = start_date
                args["end-date"] = end_date
                args["metrics"] = metrics
                if stat == DownloadAnalytics.RESOURCE_STAT:
                    args["ids"] = "ga:" + self.profile_id
                    args["prop_ids"] = self.property_id
                else:
                    args["ids"] = "ga:" + self.profile_id_gtm
                    args["prop_ids"] = self.property_id_gtm
                args["filters"] = query
                args["alt"] = "json"
                if self.segment:
                    args['segment'] = 'gaid::%s' % self.segment


                results = self._get_ga_data(args)

            except Exception as e:
                log.exception(e)
                print('EXCEPTION %s' % e)
                return dict(url=[])


            if stat == DownloadAnalytics.PACKAGE_STAT:
                packages = []
                pattern = re.compile('^' + DownloadAnalytics.PACKAGE_URL_REGEX)
                excluded_patterns = []
                for regex in DownloadAnalytics.PACKAGE_URL_EXCLUDED_REGEXS:
                    excluded_patterns.append(re.compile('^' + regex))

                rows = results if results else None
                if rows and len(rows) >= 1:
                    for row in rows:
                        if self.is_ga4:
                            path = row.get('dimensionValues', [])[0]['value']
                            pageviews = row.get('metricValues', [])[0]['value']
                        else:
                            (path, pageviews) = row
                        url = strip_off_host_prefix(path)
                        url = strip_off_language_prefix(url)
                        if not pattern.match(url):
                            continue
                        for excluded_pattern in excluded_patterns:
                            if excluded_pattern.match(url):
                                continue
                        packages.append( (url, pageviews) ) # Temporary hack
                return {stat:packages}
            elif stat == DownloadAnalytics.RESOURCE_STAT:
                resources = []
                pattern = re.compile('^' + DownloadAnalytics.RESOURCE_URL_REGEX)
                excluded_patterns = []
                for regex in DownloadAnalytics.RESOURCE_URL_EXCLUDED_REGEXS:
                    excluded_patterns.append(re.compile('^' + regex))

                rows = results if results else None
                if rows and len(rows) >= 1:
                    for row in rows:
                        if self.is_ga4:
                            event_label = row.get('dimensionValues', [])[0]['value']
                            page_path = row.get('dimensionValues', [])[1]['value']
                            total_events = row.get('metricValues', [])[0]['value']
                        else:
                            (event_label, page_path, total_events) = row
                        page_url = strip_off_host_prefix(page_path)
                        page_url = strip_off_language_prefix(page_url)
                        res_url = urllib.parse.unquote_plus(event_label)
                        if not pattern.match(page_url):
                            continue
                        for excluded_pattern in excluded_patterns:
                            if excluded_pattern.match(page_url):
                                continue
                        resources.append( (res_url, page_url, total_events) ) # Temporary hack
                return {stat:resources}
            elif stat == DownloadAnalytics.VISIT_STAT:
                rows = results if results else None
                print(rows)
                visits = 0
                if rows and len(rows) >= 1:
                    for row in rows:
                        if row:
                            if self.is_ga4:
                                visits = row.get('metricValues', [])[0]['value']
                            else:
                                visits = row[0]
                            break
                return {stat:visits}
        else:
            log.info("Not all parameters were received")
            print ("Not all parameters were received")
            return {}

    def store(self, period_name, period_complete_day, data, stat):
        if self.save_stats:
            if stat and stat == DownloadAnalytics.PACKAGE_STAT and stat in data:
                ga_model.update_dge_ga_package_stats(period_name, period_complete_day, data[stat],
                                          print_progress=self.print_progress)

            if stat and stat == DownloadAnalytics.RESOURCE_STAT and stat in data:
                ga_model.update_dge_ga_resource_stats(period_name, period_complete_day, data[stat],
                                          print_progress=self.print_progress)

            if stat and stat == DownloadAnalytics.VISIT_STAT and stat in data:
                ga_model.update_dge_ga_visit_stats(period_name, period_complete_day, data[stat],
                                          print_progress=self.print_progress)

    def _get_ga_data(self, params):
        '''Returns the GA data specified in params.
        Does all requests to the GA API and retries if needed.

        Returns a dict with the data, or dict(url=[]) if unsuccessful.
        '''
        try:
            data = self._get_ga_data_simple(params)
        except DownloadError:
            log.info('Will retry requests after a pause')
            time.sleep(300)
            try:
                data = self._get_ga_data_simple(params)
            except DownloadError:
                return dict(url=[])
            except Exception as e:
                log.exception(e)
                log.error('Uncaught exception in get_ga_data_simple (see '
                          'above)')
                return dict(url=[])
        except Exception as e:
            log.exception(e)
            log.error('Uncaught exception in get_ga_data_simple (see above)')
            return dict(url=[])
        return data


    def _get_ga_data_simple(self, params):
        '''Returns the GA data specified in params.
        Does all requests to the GA API.
        Returns a dict with the data, or raises DownloadError if unsuccessful.
        '''
        try:
            results = []
            start_index = 1
            max_results = 10000
            completed = False
            while not completed:
                if self.is_ga4:
                    start_index_ga4 = start_index - 1
                    request = {
                        "metrics": [{'name': params['metrics']}],
                        "dateRanges": [
                            {
                                "startDate": params['start-date'],
                                "endDate": params['end-date']
                            }
                        ],
                        "orderBys": [
                            {
                                "desc": True,
                                "metric": {
                                    "metricName": params['metrics']
                                },
                            }
                        ],
                        "limit": str(max_results),
                        "offset": str(start_index_ga4),
                    }

                    if 'dimensions' in params and params['dimensions']:
                        request["dimensions"] = params['dimensions']

                    if 'filters' in params and params['filters']:
                        request["dimensionFilter"] = {
                            "andGroup": {
                                "expressions": params['filters']
                            }
                        }

                    response = self.service.properties().runReport(
                        property=params['prop_ids'], body=request).execute()
                else:
                    print('filtros %s' % params['filters'])
                    print('dimensions %s' % params['dimensions'])
                    print('metrics %s' % params['metrics'])
                    print('sort %s' % params['sort'])
                    print('id %s' % params['ids'])
                    response = self.service.data().ga().get(ids=params['ids'],
                                    filters=params['filters'],
                                    dimensions=params['dimensions'],
                                    start_date=params['start-date'],
                                    start_index=start_index,
                                    max_results=max_results,
                                    metrics=params['metrics'],
                                    sort=params['sort'],
                                    end_date=params['end-date'],
                                    alt=params['alt']).execute()
                log.info('There are %d results', response.get('totalResults', 0) if response else 0)
                print ('There are %d results' % response.get('totalResults', 0) if response else 0)
                result_count = len(response.get('rows', []))
                if result_count < max_results:
                    completed = True
                results.extend(response.get('rows', []))
                start_index += max_results
                time.sleep(0.2)
            return results
        except Exception as e:
            log.error("Exception getting GA data: %s" % e)
            raise DownloadError()

    @classmethod
    def _do_ga_request(cls, params, headers):
        '''Makes a request to GA. Assumes the token init request is already done.

        Returns the response (requests object).
        On error it logs it and raises DownloadError.
        '''
        # Because of issues of invalid responses when using the ga library, we
        # are going to make these requests ourselves.
        ga_url = 'https://www.googleapis.com/analytics/v3/data/ga'
        try:
            response = requests.get(ga_url, params=params, headers=headers)
        except requests.exceptions.RequestException as e:
            log.error("Exception getting GA data: %s" % e)
            raise DownloadError()
        if response.status_code != 200:
            log.error("Error getting GA data: %s %s" % (response.status_code,
                                                        response.content))
            raise DownloadError()
        return response


global host_re
global http_re
host_re = None
http_re = None


def strip_off_host_prefix(url):
    '''Strip off the hostname that gets prefixed to the GA Path on datos.gob.es
    UA-1 but not on others.

    >>> strip_off_host_prefix('/datos.gob.es/catalogo/weekly_fuel_prices')
    '/catalogo/weekly_fuel_prices'
    >>> strip_off_host_prefix('/catalogo/weekly_fuel_prices')
    '/catalogo/weekly_fuel_prices'
    '''
    global host_re
    global http_re
    if not http_re:
        http_re = re.compile('^https?:\/\/[^\/]+\.')
    if http_re.search(url):
        # there is a dot, so must be a host name - strip it off
        url_cambiada = '/' + '/'.join(url.split('/')[3:])
        return url_cambiada
    if not host_re:
        host_re = re.compile('^\/[^\/]+\.')
    # look for a dot in the first part of the path
    if host_re.search(url):
        # there is a dot, so must be a host name - strip it off
        url_cambiada = '/' + '/'.join(url.split('/')[2:])
        return url_cambiada
    return url

def strip_off_language_prefix(url):
    '''Strip off the language that gets prefixed to the GA Path on datos.gob.es
    UA-1 but not on others.

    >>> strip_off_language_prefix('/es/catalogo/weekly_fuel_prices')
    '/catalogo/weekly_fuel_prices'
    >>> strip_off_language_prefix('/catalogo/weekly_fuel_prices')
    '/catalogo/weekly_fuel_prices'
    '''
    languages = config['ckan.locales_offered']
    if languages:
        for l in languages.split():
            prefix = '/%s/' % l
            if url.find(prefix) == 0:
                url = url[len(prefix)-1:]
                if url.endswith('/'):
                    return url[:-1]
    return url


class DownloadError(Exception):
    pass
