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

# -*- coding: utf-8 -*-

from __future__ import print_function
import datetime
import click
import os
import sys
import io
import csv
import ckanext.dge_ga_report.ga_model as ga_model
from ckan.plugins.toolkit import (config)
from ckan.model import Session
from ckanext.dge_ga_report.ga_auth import init_service
import logging
from sqlalchemy import create_engine, text
log = logging.getLogger(__name__)


def get_commands():
    return [dge_ga_report_initdb, dge_ga_report_getauthtoken, dge_ga_report_loadanalytics,dge_ga_report_generate_csv,dge_ga_report_generate_csv_admin]


@click.group("dge_ga_report_initdb")
def dge_ga_report_initdb():
    """Initialise the extension's database tables

    Usage: paster dge_ga_report_initdb
    """
    pass

@dge_ga_report_initdb.command()
def initdb():
    """Creates necessary db tables"""
    try:
        ga_model.init_tables()
        click.echo("DB tables are setup")
    except Exception as e:
        click.secho('Exception %s' % e)
        sys.exit(1)


@click.group("dge_ga_report_getauthtoken")
def dge_ga_report_getauthtoken():
    """ Get's the Google auth token

    Usage: paster dge_ga_report_getauthtoken <credentials_file>

    Where <credentials_file> is the file name containing the details
    for the service (obtained from https://code.google.com/apis/console).
    By default this is set to credentials.json
    """
    pass

@dge_ga_report_getauthtoken.command("get_token")
def get_token():
    """
    In this case we don't want a valid service, but rather just to
    force the user through the auth flow. We allow this to complete to
    act as a form of verification instead of just getting the token and
    assuming it is correct.
    """
    try:
        click.secho('Credentials file')
        init_service(config.get('ckanext-dge-ga-report.token.filepath', None))
    except Exception as e:
        click.secho('Exception %s' % e)
        sys.exit(1)


@click.group("dge_ga_report_loadanalytics")
def dge_ga_report_loadanalytics():
    """Parse data from Google Analytics API and store it
    in the ga_model

    Usage: paster dge_ga_report_loadanalytics <save|print> <kind-stat> <time-period>

    Where:

      <save-print> is:
        save        - save data in database
        print       - print data in console, not save in database

      <kind-stat> is:
        sessions    - sessions and sessions by section
        pages       - pageviews for datasets and totalevents for resources

      <time-period> is:
        latest      - (default) just the 'latest' data
        YYYY-MM     - just data for the specific month
        last_month  - just data for tha last month

    """
    pass

@dge_ga_report_loadanalytics.command("loadanalytics")
@click.argument(u"save_print", required=False, default=u"print")
@click.argument(u"kind", default=None)
@click.argument(u"time_period", required=False, default=u"latest")
@click.option(
    "-d",
    "--delete-first",
    is_flag=True,
    help="Delete data for the period first",
)
@click.option(
    "-s",
    "--stat",
    metavar="STAT",
    help="Only calulcate a particular stat (or collection of stats)",
)
def loadanalytics(save_print, kind, time_period, delete_first, stat):
    """Grab raw data from Google Analytics and save to the database"""
    init = datetime.datetime.now()
    limit_date_ga4 = datetime.datetime(int(config.get('ckanext-dge-ga-report.date.ga4.year', None)), int(
        config.get('ckanext-dge-ga-report.date.ga4.month', None)), 2, 0, 0, 0)

    try:
        from ckanext.dge_ga_report.download_analytics import DownloadAnalytics
        from .ga_auth import (init_service, get_profile_id)

        '''Analyzing whether the specified period is before or after GA4.'''
        if time_period == 'latest' or time_period == 'last_month':
            is_ga4 = True
        else:
            specific_month = datetime.datetime.strptime(time_period, '%Y-%m')
            is_ga4 = limit_date_ga4 < specific_month

        try:
            svc = init_service(config.get('ckanext-dge-ga-report.token.filepath', None), is_ga4)
        except TypeError:
            click.echo ('Unable to create a service. Have you correctly run the getauthtoken task and '
                    'specified the correct token file in the CKAN config under '
                    '"ckanext-dge-ga-report.token.filepath"?')
            sys.exit(1)

        save = True if save_print == 'save' else False

        if kind is None or kind not in DownloadAnalytics.KIND_STATS:
            click.secho(('A valid kind of statistics that you want to load must be '
                    'specified: %s' % DownloadAnalytics.KIND_STATS))
            sys.exit(1)

        '''If ga4, profile_id is not neccessary'''
        if is_ga4:
           profile_id = ""
           profile_id_gtm = ""
        else:
            webPropertyId_gtm = config.get('ckanext-dge-ga-report.prop_id_gtm')
            view_id_gtm = config.get('ckanext-dge-ga-report.view_id_gtm', None)
            profile_id_gtm = get_profile_id(svc, webPropertyId_gtm, view_id_gtm)
            if kind == 'pages':
                webPropertyId = config.get('ckanext-dge-ga-report.prop_id')
                view_id = config.get('ckanext-dge-ga-report.view_id', None)
                profile_id = get_profile_id(svc, webPropertyId, view_id)
            else:
                profile_id = ""

        downloader = DownloadAnalytics(service=svc, token=None, profile_id=profile_id, profile_id_gtm=profile_id_gtm,
                                       delete_first=False, stat=None, print_progress=True, kind_stats=kind, save_stats=save, is_ga4=is_ga4)

        if time_period == 'latest':
            downloader.latest()
        elif time_period == 'last_month':
            now = datetime.datetime.now()
            if now.month == 1:
                last_month = datetime.datetime(now.year-1, 12, 1, 0, 0, 0)
            else:
                last_month = datetime.datetime(now.year, now.month-1, 1, 0, 0, 0)
            downloader.specific_month(last_month)
        else:
            # The month to use
            for_date = datetime.datetime.strptime(time_period, '%Y-%m')
            downloader.specific_month(for_date)
    except Exception as err:
        click.secho('Exception %s' % err)
        sys.exit(1)
    finally:
        click.echo('End DgeGaReportLoadAnalytics command with args. Executed command in milliseconds')
    sys.exit(0)



def generar_csv_desde_sql(sql_query,file_path):
    """
    Genera un archivo CSV a partir de una consulta SQL.

    Args:
    - sql_query (str): Consulta SQL que devuelve los datos para el CSV.

    Returns:
    - str: El contenido CSV generado.
    """
    result = Session.execute(sql_query)
    log.info("Consulta SQL ejecutada")

    rows = result.fetchall()
    log.info("Filas obtenidas: %d", len(rows))

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)

        columns = result.keys()

        writer.writerow(columns)
        log.info("Cabecera escrita")

        for row in rows:
            writer.writerow(row)
        log.info("Datos escritos al archivo")

    log.info("CSV generado y guardado en: %s", file_path)

def generar_csv_desde_mysql(mysql_query,file_path):
    """
    Genera un archivo CSV a partir de una consulta SQL.

    Args:
    - sql_query (str): Consulta SQL que devuelve los datos para el CSV.

    Returns:
    - str: El contenido CSV generado.
    """
    engine = create_engine(config.get('ckanext.dge_drupal_users.connection', None))
    result = None
    rows = []

    with engine.connect() as connection:
        result = engine.execute(mysql_query)
        rows = result.fetchall()
    log.info("Filas obtenidas: %d", len(rows))

    with open(file_path, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        columns = result.keys()
        writer.writerow(columns)
        log.info("Cabecera escrita")
        for row in rows:
            writer.writerow(row)
        log.info("Datos escritos al archivo")

    log.info("CSV generado y guardado en: %s", file_path)

def generar_csv_catalogo_datos_publico_evolucion():
    sql = """
    select
    year_month as year,
    num_datasets as value
    from dge_dashboard_published_datasets
    where key like 'total'
    order by year_month asc;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_datos_publico_evolucion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)



def generar_csv_catalogo_datos_publico_por_nivel_administracion():
    sql = """
    select
        TO_CHAR(NOW(), 'YYYY-MM-DD') as "date",
        CASE admin_level
            WHEN 'A' THEN 'Administración Autonómica'
            WHEN 'E' THEN 'Administración del Estado'
            WHEN 'L' THEN 'Administración Local'
            WHEN 'P' THEN 'Entidad Privada'
            WHEN 'U' THEN 'Universidades'
            WHEN 'G/I' THEN 'Otras instituciones'
        END AS administration_level,
        SUM(num_datasets) AS num_datasets
        FROM (
            select
            COUNT(p.title) AS num_datasets,
            CASE
                WHEN substring(ge.value, 1, 1) IN ('G', 'I') THEN 'G/I'
                ELSE substring(ge.value, 1, 1)
            END AS admin_level
            FROM
            package p
            INNER JOIN
            package_extra pe ON p.id = pe.package_id AND pe.key = 'publisher'
            LEFT JOIN
            "group" grp ON pe.value = grp.id
            LEFT JOIN
            group_extra ge ON grp.id = ge.group_id AND ge.key = 'C_ID_UD_ORGANICA'
            WHERE
            p.type = 'dataset'
            AND p.state = 'active'
            AND p.private IS FALSE
            GROUP BY
            admin_level
            ) AS grouped
        GROUP BY  admin_level
        ORDER BY
            CASE admin_level
                WHEN 'A' THEN 1
                WHEN 'E' THEN 2
                WHEN 'G/I' THEN 3
                WHEN 'L' THEN 4
                WHEN 'P' THEN 5
                WHEN 'U' THEN 6
                ELSE 7
            end;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_datos_por_nivel_administracion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)


def generar_csv_catalogo_datos_publico_por_categoria():
    sql = """
    select s.date ,
   		CASE s.theme
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/sector-publico' THEN 'Sector público'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/empleo' THEN 'Empleo'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/demografia' THEN 'Demografía'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/sociedad-bienestar' THEN 'Sociedad y bienestar'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/educacion' THEN 'Educación'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/medio-ambiente' THEN 'Medio ambiente'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/economia' THEN 'Economía'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/salud' THEN 'Salud'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/hacienda' THEN 'Hacienda'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/legislacion-justicia' THEN 'Legislación y justicia'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/turismo' THEN 'Turismo'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/medio-rural-pesca' THEN 'Medio Rural'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/vivienda' THEN 'Vivienda'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/transporte' THEN 'Transporte'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/ciencia-tecnologia' THEN 'Ciencia y tecnología'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/urbanismo-infraestructuras' THEN 'Urbanismo e infraestructuras'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/cultura-ocio' THEN 'Cultura y ocio'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/comercio' THEN 'Comercio'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/seguridad' THEN 'Seguridad'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/energia' THEN 'Energía'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/industria' THEN 'Industria'
        WHEN 'http://datos.gob.es/kos/sector-publico/sector/deporte' THEN 'Deporte'
        ELSE 'Otro'
    END AS theme,
    s.value
   		from
			(select
	   		TO_CHAR(NOW(), 'YYYY-MM-DD') as "date",
			jsonb_array_elements_text(pe.value::jsonb) AS theme,
			count(p.title) as value
	        from package p inner join package_extra pe on pe.package_id = p.id
	        where p.type = 'dataset' and p.state = 'active' and p.private is false and pe.key = 'theme'
	        group by jsonb_array_elements_text(pe.value::jsonb)
	        order by value desc)s
	        where theme like '%datos.gob.es/kos/%'
	order by value desc, theme asc;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_datos_por_categoria.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)


def generar_csv_catalogo_datos_publico_por_formato_distribucion():
    sql = """
    select
        TO_CHAR(NOW(), 'YYYY-MM-DD') as "date",
        CASE r.format
        WHEN 'text/csv' THEN 'CSV'
        WHEN 'application/json' THEN 'JSON'
        WHEN 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' THEN 'XLSX'
        WHEN 'text/html' THEN 'HTML'
        WHEN 'text/pc-axis' THEN 'PC-Axis'
        WHEN 'application/vnd.ms-excel' THEN 'XLS'
        WHEN 'application/pdf' THEN 'PDF'
        WHEN 'image/png' THEN 'PNG'
        WHEN 'text/tab-separated-values' THEN 'TSV'
        WHEN 'text/xml' THEN 'XML'
        WHEN 'application/xml' THEN 'XML'
        WHEN 'image/jpeg' THEN 'JPG'
        WHEN 'application/vnd.google-earth.kml+xml' THEN 'KML'
        WHEN 'text/wms' THEN 'WMS'
        WHEN 'image/tiff' THEN 'TIFF'
        WHEN 'application/zip' THEN 'ZIP'
        WHEN 'text/ascii' THEN 'ASCII'
        WHEN 'text/plain' THEN 'plain'
        WHEN 'application/api' THEN 'API'
        WHEN 'application/x-zipped-shp' THEN 'SHP'
        WHEN 'text/turtle' THEN 'RDF-Turtle'
        WHEN 'application/vnd.oasis.opendocument.spreadsheet' THEN 'ODS'
        WHEN 'application/rdf+xml' THEN 'RDF-XML'
        WHEN 'application/vnd.google-earth.kmz' THEN 'KMZ'
        WHEN 'application/vnd.geo+json' THEN 'GeoJSON'
        WHEN 'application/gml+xml' THEN 'GML'
        WHEN 'application/octet-stream' THEN 'OCTET-STREAM'
        WHEN 'application/ld+json' THEN 'JSON-LD'
        WHEN 'application/atom+xml' THEN 'Atom'
        WHEN 'application/geopackage+sqlite3' THEN 'GeoPackage'
        WHEN 'application/rss+xml' THEN 'RSS'
        WHEN 'text/n3' THEN 'RDF-N3'
        WHEN 'application/vnd.ogc.wms_xml' THEN 'WMS-XML'
        WHEN 'application/scorm' THEN 'SCORM'
        WHEN 'application/elp' THEN 'ELP'
        WHEN 'application/x-turtle' THEN 'TURTLE'
        WHEN 'text/rdf+n3' THEN 'N3'
        WHEN 'application/netcdf' THEN 'NetCDF'
        WHEN 'application/xhtml+xml' THEN 'XHTML'
        WHEN 'application/sparql-query' THEN 'SPARQL'
        WHEN 'application/x-zip-compressed' THEN 'ZIP'
        WHEN 'application/geo+pdf' THEN 'GeoPDF'
        WHEN 'application/geo+json' THEN 'GeoJSON'
        WHEN 'text/wfs' THEN 'WFS'
        WHEN 'application/javascript' THEN 'JSON-P'
        WHEN 'application/gpx+xml' THEN 'GPX'
        WHEN 'text/calendar' THEN 'Calendar'
        WHEN 'application/gzip' THEN 'GZIP'
        WHEN 'application/ecw' THEN 'ECW'
        WHEN 'application/msaccess' THEN 'MDB'
        WHEN 'image/vnd.dwg' THEN 'DWG'
        WHEN 'application/vnd.oasis.opendocument.text' THEN 'ODT'
        WHEN 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' THEN 'DOCX'
        WHEN 'application/marc' THEN 'MARC'
        WHEN 'application/x-tmx+xml' THEN 'TMX'
        WHEN 'image/jp2' THEN 'JP2'
        WHEN 'text/wcs' THEN 'WCS'
        WHEN 'application/solr' THEN 'Solr'
        WHEN 'image/vnd.djvu' THEN 'DjVu'
        WHEN 'application/x-qgis' THEN 'QGIS'
        WHEN 'application/msword' THEN 'DOC'
        WHEN 'text/rtf' THEN 'RTF'
        WHEN 'image/vnd.dgn' THEN 'DGN'
        WHEN 'application/xbrl' THEN 'XBRL'
        WHEN 'application/ecmascript' THEN 'ECMAScript'
        WHEN 'application/las' THEN 'LAS'
        WHEN 'application/x-json' THEN 'JSON'
        WHEN 'application/x-rar-compressed' THEN 'RAR'
        WHEN 'application/x-tbx+xml' THEN 'TBX'
        WHEN 'application/vnd.apache.parquet' THEN 'Parquet'
        WHEN 'image/svg+xml' THEN 'SVG'
        WHEN 'application/dbf' THEN 'DBF'
        WHEN 'application/epub+zip' THEN 'ePub'
        WHEN 'application/sparql-results+xml' THEN 'SPARQL-XML'
        WHEN 'text/xml+georss' THEN 'GeoRSS'
        WHEN 'application/sparql-results+json' THEN 'SPARQL-JSON'
        WHEN 'application/csw' THEN 'CSW'
        WHEN 'application/dxf' THEN 'DXF'
        WHEN 'x-lml/x-gdb' THEN 'GDB'
        WHEN 'application/mp4' THEN 'MP4'
        WHEN 'application/soap+xml' THEN 'SOAP'
        ELSE r.format
        END AS format,
        count(p.title) as value
    from package p inner join resource r ON p.id = r.package_id
    where
        p.type = 'dataset'
        and p.state = 'active'
        and p.private = FALSE
        and r.state = 'active'
    group by r.format
    order by value desc;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_datos_por_formato_distribucion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)

def generar_csv_contenido_publico_por_tipo():
    sql = """
    select
	   	year_month AS "date",
	  	SUM(num_contents) FILTER (WHERE content_type = 'app') AS app,
	  	SUM(num_contents) FILTER (WHERE content_type = 'initiative') AS initiative,
	  	SUM(num_contents) FILTER (WHERE content_type = 'request') AS request,
	  	SUM(num_contents) FILTER (WHERE content_type = 'success') AS success
	from dge_dashboard_drupal_contents
	where content_type IN ('app', 'initiative', 'request', 'success')
	GROUP BY year_month
	ORDER BY year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'contenido_publico_por_tipo.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)



def generar_csv_visitas_publico_evolucion():
    sql = """
    SELECT year_month as date, sessions as value FROM dge_ga_visits WHERE key = 'all' order by year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'visitas_publico_evolucion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)
 

def generar_csv_visitas_publico_catalogo_nacional():
    sql = """
    SELECT year_month as date,  sessions as catalogo  FROM dge_ga_visits WHERE key_value = 'catalogo' order by year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'visitas_publico_catalogo_nacional.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)


def generar_csv_visitas_publico_contenido():
    sql = """
    SELECT
  		  year_month AS date,
  		  SUM(CASE WHEN key_value = 'aplicaciones' THEN sessions ELSE 0 END) AS aplicaciones,
  		  SUM(CASE WHEN key_value = 'documentacion' THEN sessions ELSE 0 END) AS documentacion,
  		  SUM(CASE WHEN key_value = 'empresas-reutilizadoras' THEN sessions ELSE 0 END) as "empresas-reutilizadoras",
  		  SUM(CASE WHEN key_value = 'entrevistas' THEN sessions ELSE 0 END) AS entrevistas,
		  SUM(CASE WHEN key_value = 'eventos' THEN sessions ELSE 0 END) AS eventos,
  		  SUM(CASE WHEN key_value = 'iniciativas' THEN sessions ELSE 0 END) AS iniciativas,
		  SUM(CASE WHEN key_value = 'noticias' THEN sessions ELSE 0 END) AS noticias,
		  SUM(CASE WHEN key_value = 'peticiones-datos' THEN sessions ELSE 0 END) AS "peticiones-datos",
		  SUM(CASE WHEN key_value = 'blog_blog' THEN sessions ELSE 0 END) AS blog_blog
		FROM dge_ga_visits
		WHERE key_value IN ('aplicaciones', 'blog_blog', 'empresas-reutilizadoras', 'entrevistas', 'eventos',
		  'documentacion', 'iniciativas', 'noticias', 'peticiones-datos'
		)
		GROUP BY year_month
		ORDER BY year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'visitas_publico_contenido.csv')
    csv_content = generar_csv_desde_sql(sql,file_path)
 

def generar_csv_visitas_publico_sectores():
    sql = """
     SELECT
  		  year_month AS date,
  		  SUM(CASE WHEN key_value = 'agricultura' THEN sessions ELSE 0 END) AS agricultura,
  		  SUM(CASE WHEN key_value = 'cultura' THEN sessions ELSE 0 END) AS cultura,
  		  SUM(CASE WHEN key_value = 'educacion' THEN sessions ELSE 0 END) AS educacion,
  		  SUM(CASE WHEN key_value = 'transporte' THEN sessions ELSE 0 END) AS transporte,
		  SUM(CASE WHEN key_value = 'salud-bienestar' THEN sessions ELSE 0 END) AS "salud-bienestar",
  		  SUM(CASE WHEN key_value = 'turismo' THEN sessions ELSE 0 END) AS turismo,
		  SUM(CASE WHEN key_value = 'justicia-sociedad' THEN sessions ELSE 0 END) AS "justicia-sociedad"
    FROM dge_ga_visits WHERE key_value in
        ('agricultura' , 'cultura', 'educacion', 'justicia-sociedad', 'salud-bienestar', 'transporte', 'turismo')
	GROUP BY year_month
	ORDER BY year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')
    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'visitas_publico_sectores.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)



def generar_csv_visitas_publico_mas_vistos():
    sql = """
    WITH ranked_data AS (
	    SELECT
	        s1.year_month,
	        CASE
	            WHEN s1.year_month = 'All' THEN 'Total acumulado'
	            ELSE
	                CASE
	                    WHEN EXTRACT(MONTH FROM TO_DATE(s1.year_month, 'YYYY-MM')) = 2 AND CAST(s1.end_day AS INTEGER) IN (28, 29)
	                        THEN TO_CHAR(TO_DATE(s1.year_month, 'YYYY-MM'), 'TMMonth') || ' ' || TO_CHAR(TO_DATE(s1.year_month, 'YYYY-MM'), 'YYYY')
	                    WHEN CAST(s1.end_day AS INTEGER) IN (30, 31)
	                        THEN TO_CHAR(TO_DATE(s1.year_month, 'YYYY-MM'), 'TMMonth') || ' ' || TO_CHAR(TO_DATE(s1.year_month, 'YYYY-MM'), 'YYYY')
	                    ELSE TO_CHAR(TO_DATE(s1.year_month, 'YYYY-MM'), 'TMMonth') || ' ' || TO_CHAR(TO_DATE(s1.year_month, 'YYYY-MM'), 'YYYY') || ' (hasta el ' || s1.end_day || ')'
	                END
	        END AS "Mes",
	        CONCAT('https://datos.gob.es/es/catalogo/',p.name) as "Url",
	        p.title AS "Conjunto de datos",
	        g.title AS "Publicador",
	        s1.pageviews AS "Visitas",
	        ROW_NUMBER() OVER (
	            PARTITION BY s1.year_month
	            ORDER BY s1.pageviews DESC
	        ) AS rn
	    FROM
	        "group" g
	        INNER JOIN dge_ga_packages s1 ON g.id = s1.publisher_id
	        INNER JOIN package p ON p.name = s1.package_name
	    WHERE
	        s1.publisher_id IS NOT null and p.private is false
	)
	SELECT
	    "Mes", "Url", "Conjunto de datos", "Publicador", "Visitas"
	FROM
	    ranked_data
	WHERE
	    rn <= 10
	ORDER BY
	    year_month DESC, rn;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'visitas_publico_mas_vistos.csv')
    csv_content = generar_csv_desde_sql(sql,file_path)



@click.group("dge_ga_report_generate_csv")
def dge_ga_report_generate_csv():
    """ Get's the Google auth token

    Usage: paster dge_ga_report_generate_csv <credentials_file>

    Where <credentials_file> is the file name containing the details
    for the service (obtained from https://code.google.com/apis/console).
    By default this is set to credentials.json
    """
    pass

@dge_ga_report_generate_csv.command("catalogo_datos_publico_evolucion")
def visitas_publico_evolucion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_datos_publico_evolucion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv.command("catalogo_datos_publico_nivel_administracion")
def catalogo_datos_publico_nivel_administracion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_datos_publico_por_nivel_administracion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv.command("catalogo_datos_publico_categoria")
def catalogo_datos_publico_categoria():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_datos_publico_por_categoria()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv.command("catalogo_datos_publico_formato_distribucion")
def catalogo_datos_publico_formato_distribucion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_datos_publico_por_formato_distribucion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)


@dge_ga_report_generate_csv.command("contenido_publico_por_tipo")
def contenido_publico_por_tipo():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_contenido_publico_por_tipo()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)


@dge_ga_report_generate_csv.command("visitas_publico_evolucion")
def visitas_publico_evolucion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_visitas_publico_evolucion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv.command("visitas_publico_catalogo_nacional")
def visitas_publico_catalogo_nacional():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_visitas_publico_catalogo_nacional()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv.command("visitas_publico_contenido")
def visitas_publico_contenido():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_visitas_publico_contenido()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)    

@dge_ga_report_generate_csv.command("visitas_publico_sectores")
def visitas_publico_sectores():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_visitas_publico_sectores()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)   

@dge_ga_report_generate_csv.command("visitas_publico_mas_vistos")
def visitas_publico_mas_vistos():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_visitas_publico_mas_vistos()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)



def generar_csv_catalogo_admin_evolucion_nivel_administracion():
    sql = """
    SELECT
	    year_month AS "year",
	    -- Para cada valor de key_value, sumar o mostrar num_datasets
	    SUM(CASE WHEN key_value = 'A' THEN num_datasets END) AS "A",
	    SUM(CASE WHEN key_value = 'E' THEN num_datasets END) AS "E",
	    SUM(CASE WHEN key_value = 'L' THEN num_datasets END) AS "L",
	    SUM(CASE WHEN key_value = 'U' THEN num_datasets END) AS "U",
	    SUM(CASE WHEN key_value IN ('I', 'G', 'X') THEN num_datasets END) AS "I",
	    SUM(CASE WHEN key_value = 'P' THEN num_datasets END) AS "P"
	FROM dge_dashboard_published_datasets
	WHERE "key" = 'administration_level'
	GROUP BY year_month
	ORDER BY year_month ASC;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_admin_evolucion_nivel_administracion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)



def generar_csv_catalogo_admin_organismos():
    sql_generador = """
    WITH last_complete_month AS (
  SELECT date_trunc('month', now()) - interval '1 day' AS last_day_prev_month
),
months AS (
  SELECT to_char(date_trunc('month', d), 'YYYY-MM') AS year_month
  FROM generate_series('2016-11-01'::date, (SELECT last_day_prev_month FROM last_complete_month), interval '1 month') d
),
cols AS (
  SELECT string_agg(
    format('COALESCE(SUM(num_datasets) FILTER (WHERE year_month = %L)) AS "%s"', year_month, year_month),
    ', ' ORDER BY year_month
  ) AS columns_list
  FROM months
)
SELECT format(
  $$
  SELECT
    g.title AS "Organismo",
    %s
  FROM dge_dashboard_published_datasets
  INNER JOIN "group" g ON dge_dashboard_published_datasets.key_value = g.id
  WHERE dge_dashboard_published_datasets."key" = 'organization_id'
  GROUP BY g.title
  ORDER BY g.title;
  $$, columns_list)
FROM cols;
    """

    result = Session.execute(text(sql_generador))
    query_generada = result.scalar()
    if not query_generada:
        log.error("No se pudo generar la consulta dinámica")
        raise Exception("No se pudo generar la consulta dinámica")

    dashboard_csv_dir = config.get('ckanext-dge-ga-report.routing_dashboard_csv', '')

    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir, date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_admin_organismos.csv')

    generar_csv_desde_sql(query_generada, file_path)


def generar_csv_catalogo_admin_distribuciones():
    sql = """
    SELECT
    TO_CHAR(TO_DATE(year_month, 'YYYY-MM'), 'TMMonth YYYY') AS "Mes",
    key_value AS "Número de distribuciones por conjunto de datos",
    num_datasets AS "Número total de conjuntos de datos"
	FROM
	    dge_dashboard_published_datasets
	WHERE
	    key = 'num_resources'
	order by key_value, year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'catalogo_admin_distribuciones.csv')
    csv_content = generar_csv_desde_sql(sql,file_path)



def generar_csv_visitas_admin_mas_vistos():
    sql = """
          SELECT 
    year_month AS fecha,   
    title as conjunto_datos, 
    publisher as publicador, 
    visits as visitas
    FROM 
        (
            SELECT 
                s1.year_month,
                s1.end_day,
                p.name,
                p.title AS title,
                g.title AS publisher,
                s1.pageviews AS visits,
                pe.value AS sector
            FROM 
                dge_ga_packages s1
                JOIN package p ON p.name = s1.package_name
                JOIN "group" g ON g.id = s1.publisher_id
                LEFT JOIN (
                    SELECT 
                        package_id,  
                        value 
                    FROM 
                        package_extra 
                    WHERE 
                        "key" = 'theme'
                ) pe ON pe.package_id = p.id
            WHERE 
                s1.organization_id IS NOT NULL
                AND s1.publisher_id IS NOT NULL
                AND p.private = FALSE
        ) s3;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'visitas_admin_mas_vistos.csv')
    csv_content = generar_csv_desde_sql(sql,file_path)


def generar_csv_contenidos_admin_comentarios_recibidos():
    sql = """
     select year_month as year,
       sum(case when content_type = 'content_comments' then num_contents else 0 end) as content_comments,
       sum(case when content_type = 'dataset_comments' then num_contents else 0 end) as dataset_comments
    from dge_dashboard_drupal_contents
    where key = 'total' and (content_type = 'dataset_comments' or content_type = 'content_comments')
    group by year_month
    order by year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'contenidos_admin_comentarios_recibidos.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)


def generar_csv_publicadores_admin_evolucion():
    sql = """
    select
		year_month as year,
		harvester_publishers,
		manual_loading_publishers,
		"both"
	from
		dge_dashboard_publishers
	order by year_month;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public',dashboard_csv_dir,date)
    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'publicadores_admin_evolucion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)


def generar_csv_publicadores_admin_nivel_administracion():
    sql = """
    SELECT
    TO_CHAR(NOW(), 'yyyy-MM-dd') as "date",
    org_levels.adm_level as "category",
    COUNT(CASE WHEN publishers.type = 'federacion' THEN 1 END) as "harvester_publishers",
    COUNT(CASE WHEN publishers.type = 'manual' THEN 1 END) as "manual_loading_publishers",
    COUNT(CASE WHEN publishers.type = 'ambas' THEN 1 END) as "both"
FROM (
    SELECT
        pub,
        CASE
            WHEN COUNT(DISTINCT CASE WHEN publisher_type = 'manual_loading_publishers' THEN 1 END) > 0
                 AND COUNT(DISTINCT CASE WHEN publisher_type = 'harvester_publishers' THEN 1 END) > 0
                THEN 'ambas'
            WHEN COUNT(DISTINCT CASE WHEN publisher_type = 'manual_loading_publishers' THEN 1 END) > 0
                THEN 'manual'
            WHEN COUNT(DISTINCT CASE WHEN publisher_type = 'harvester_publishers' THEN 1 END) > 0
                THEN 'federacion'
            ELSE 'unknown'
        END AS type
    FROM (
        SELECT
            s3.pub,
            CASE
                WHEN s4.owner_org IS NULL OR s3.guid = false
                    THEN 'manual_loading_publishers'
                ELSE 'harvester_publishers'
            END AS publisher_type
        FROM (
            SELECT DISTINCT
                s1.owner_org,
                s2.pub,
                s2.guid
            FROM (
                SELECT
                    p1.id,
                    p1.owner_org
                FROM
                    package p1
                WHERE
                    p1.state = 'active'
                    AND p1.type = 'dataset'
                    AND p1.private = false
            ) s1
            JOIN (
                SELECT
                    pe1.value AS pub,
                    pe1.package_id,
                    CASE
                        WHEN pe2.package_id IS NULL THEN false
                        ELSE true
                    END AS guid
                FROM (
                    SELECT
                        pe.value,
                        pe.package_id
                    FROM
                        package_extra pe
                    WHERE
                        pe.state = 'active'
                        AND pe.key = 'publisher'
                ) pe1
                LEFT JOIN (
                    SELECT
                        pe.package_id
                    FROM
                        package_extra pe
                    WHERE
                        pe.state = 'active'
                        AND pe.key = 'guid'
                ) pe2 ON pe1.package_id = pe2.package_id
            ) s2 ON s1.id = s2.package_id
        ) s3
        LEFT JOIN (
            SELECT DISTINCT
                p2.owner_org
            FROM
                package p2
            WHERE
                p2.state = 'active'
                AND p2.type = 'harvest'
                AND p2.private = false
        ) s4 ON s4.owner_org = s3.owner_org
    ) sub
    GROUP BY pub
) publishers
LEFT JOIN (
    SELECT
        g.id AS pub,
        CASE
            WHEN SUBSTRING(ge.value, 1, 1) = 'A' THEN 'Administración Autonómica'
            WHEN SUBSTRING(ge.value, 1, 1) = 'E' THEN 'Administración del Estado'
            WHEN SUBSTRING(ge.value, 1, 1) = 'L' THEN 'Administración Local'
            WHEN SUBSTRING(ge.value, 1, 1) = 'U' THEN 'Universidades'
            WHEN SUBSTRING(ge.value, 1, 1) = 'P' THEN 'Entidad Privada'
            WHEN SUBSTRING(ge.value, 1, 1) IN ('G', 'I') THEN 'Otras instituciones'
            ELSE 'No clasificado'
        END AS adm_level
    FROM
        "group" g
    JOIN group_extra ge ON ge.group_id = g.id
    WHERE
        g.state = 'active'
        AND g.type = 'organization'
        AND ge.state = 'active'
        AND ge.key = 'C_ID_UD_ORGANICA'
) org_levels ON publishers.pub = org_levels.pub
GROUP BY org_levels.adm_level
ORDER BY CASE org_levels.adm_level
	WHEN 'Administración del Estado' THEN 1
    WHEN 'Administración Autonómica' THEN 2
    WHEN 'Administración Local' THEN 3
    WHEN 'Universidades' THEN 4
    WHEN 'Otras instituciones' THEN 5
    WHEN 'Entidad Privada' THEN 6
    ELSE 7
END;
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir,date)

    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'publicadores_admin_nivel_administracion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)

def generar_csv_publicadores_admin_nivel_administracion_forma_actualizacion():
    sql = """
    SELECT
	TO_CHAR(NOW(), 'yyyy-MM-dd') as "Fecha de actualización de los datos",
    g.title AS "Organismo",
    org_levels.adm_level as "Nivel de administración",
    publishers.type as "Tipo de actualización"
FROM (
    SELECT
        pub,
        CASE
            WHEN COUNT(DISTINCT CASE WHEN publisher_type = 'manual_loading_publishers' THEN 1 END) > 0
                 AND COUNT(DISTINCT CASE WHEN publisher_type = 'harvester_publishers' THEN 1 END) > 0
                THEN 'ambas'
            WHEN COUNT(DISTINCT CASE WHEN publisher_type = 'manual_loading_publishers' THEN 1 END) > 0
                THEN 'manual'
            WHEN COUNT(DISTINCT CASE WHEN publisher_type = 'harvester_publishers' THEN 1 END) > 0
                THEN 'federacion'
            ELSE 'unknown'
        END AS type
    FROM (
        SELECT
            s3.pub,
            CASE
                WHEN s4.owner_org IS NULL OR s3.guid = false
                    THEN 'manual_loading_publishers'
                ELSE 'harvester_publishers'
            END AS publisher_type
        FROM (
            SELECT DISTINCT
                s1.owner_org,
                s2.pub,
                s2.guid
            FROM (
                SELECT
                    p1.id,
                    p1.owner_org
                FROM
                    package p1
                WHERE
                    p1.state = 'active'
                    AND p1.type = 'dataset'
                    AND p1.private = false
            ) s1
            JOIN (
                SELECT
                    pe1.value AS pub,
                    pe1.package_id,
                    CASE
                        WHEN pe2.package_id IS NULL THEN false
                        ELSE true
                    END AS guid
                FROM (
                    SELECT
                        pe.value,
                        pe.package_id
                    FROM
                        package_extra pe
                    WHERE
                        pe.state = 'active'
                        AND pe.key = 'publisher'
                ) pe1
                LEFT JOIN (
                    SELECT
                        pe.package_id
                    FROM
                        package_extra pe
                    WHERE
                        pe.state = 'active'
                        AND pe.key = 'guid'
                ) pe2 ON pe1.package_id = pe2.package_id
            ) s2 ON s1.id = s2.package_id
        ) s3
        LEFT JOIN (
            SELECT DISTINCT
                p2.owner_org
            FROM
                package p2
            WHERE
                p2.state = 'active'
                AND p2.type = 'harvest'
                AND p2.private = false
        ) s4 ON s4.owner_org = s3.owner_org
    ) sub
    GROUP BY pub
) publishers
LEFT JOIN (
    SELECT
        g.id AS pub,
        CASE
            WHEN SUBSTRING(ge.value, 1, 1) = 'A' THEN 'Administración Autonómica'
            WHEN SUBSTRING(ge.value, 1, 1) = 'E' THEN 'Administración del Estado'
            WHEN SUBSTRING(ge.value, 1, 1) = 'L' THEN 'Administración Local'
            WHEN SUBSTRING(ge.value, 1, 1) = 'U' THEN 'Universidades'
            WHEN SUBSTRING(ge.value, 1, 1) = 'P' THEN 'Entidad Privada'
            WHEN SUBSTRING(ge.value, 1, 1) IN ('G', 'I') THEN 'Otras instituciones'
            ELSE 'No clasificado'
        END AS adm_level
    FROM
        "group" g
    JOIN group_extra ge ON ge.group_id = g.id
    WHERE
        g.state = 'active'
        AND g.type = 'organization'
        AND ge.state = 'active'
        AND ge.key = 'C_ID_UD_ORGANICA'
) org_levels ON publishers.pub = org_levels.pub
LEFT JOIN "group" g ON g.id = publishers.pub
ORDER BY g.title collate "C";
    """
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir,date)

    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'publicadores_admin_nivel_administracion_forma_actualizacion.csv')

    csv_content = generar_csv_desde_sql(sql,file_path)

def generar_csv_contenido_por_tipo_administracion():
    sql = text("""
    SELECT  
    DATE_FORMAT(CURRENT_TIMESTAMP, '%d/%m/%Y') AS "Fecha de actualización de los datos",
    CASE
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'aplicacion' THEN 'Aplicación'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'blog' THEN 'Blog'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'peticion_de_datos' THEN 'Consulta de disponiblidad de datos'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'desafio_aporta' THEN 'Desafío Aporta'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'documentacion' THEN 'Documentación'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'empresa_reutilizadora' THEN 'Empresa reutilizadora'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'entrevista' THEN 'Entrevista'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'evento' THEN 'Evento'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'faq' THEN 'FAQ'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'iniciativa' THEN 'Iniciativa'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'noticia' THEN 'Noticia'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'ponentes_aporta' THEN 'Ponentes Aporta'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'page' THEN 'Página'
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'sectores' THEN 'Sectores'
        ELSE CONVERT(n.`type` USING utf8mb4)
    END AS "Tipo de contenido",
    COUNT(DISTINCT nfd.title) AS "Número de contenidos"
    FROM node n
    INNER JOIN node_field_data nfd ON n.type = nfd.type
    WHERE status = 1 AND nfd.langcode = 'es'
    GROUP BY n.type
    ORDER BY CASE
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'aplicacion' THEN 1
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'blog' THEN 2
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'peticion_de_datos' THEN 3
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'desafio_aporta' THEN 4
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'documentacion' THEN 5
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'empresa_reutilizadora' THEN 6
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'entrevista' THEN 7
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'evento' THEN 8
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'faq' THEN 9
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'iniciativa' THEN 10
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'noticia' THEN 11
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'ponentes_aporta' THEN 12
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'page' THEN 13
        WHEN CONVERT(n.`type` USING utf8mb4) COLLATE utf8mb4_unicode_ci = 'sectores' THEN 14
        ELSE 99
    END;
    """)
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir,date)

    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'contenido_por_tipo_administracion.csv')

    csv_content = generar_csv_desde_mysql(sql,file_path)


def generar_csv_disponibilidad_datos_por_estado():
    sql = text("""
    select
        DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d') AS "date",
            d.name as "state",
            count(*) as "value"
        from
            node_field_data n
        inner join node__field_estado nfe on
            n.nid = nfe.entity_id
        inner join taxonomy_term_field_data d on d.tid = nfe.field_estado_target_id
        where
            n.`type` like 'peticion_de_datos' and n.langcode = 'es'
            and n.status = 1
            and d.langcode = 'es'
        group by d.name ;
    """)
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir,date)

    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'disponibilidad_datos_por_estado.csv')

    csv_content = generar_csv_desde_mysql(sql,file_path)


def generar_csv_usuarios_por_organismo():
    sql = text("""
    SELECT
        DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d') AS "Fecha de actualización de los datos",
        d.name as "Organismo",
        count(*) as "Número total de usuarios"
        FROM
        users_field_data u
        inner join user__field_organizacion ufo on u.uid = ufo.entity_id
        inner join taxonomy_term__field_ckan_organization_name t on t.entity_id = ufo.field_organizacion_target_id
        inner join taxonomy_term_field_data d on d.tid = ufo.field_organizacion_target_id
        where u.status = 1
        GROUP by d.name
        order by d.name asc;
    """)
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir,date)

    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'usuarios_por_organismo.csv')

    csv_content = generar_csv_desde_mysql(sql,file_path)


def generar_csv_usuarios_por_nivel_administracion():
    sql = text("""
    SELECT  
        DATE_FORMAT(CURRENT_TIMESTAMP, '%Y-%m-%d') AS "date",
        s1.adm_level,
        SUM(s1.total_users) AS "num_users"
        FROM (
        SELECT
            CASE
            WHEN SUBSTR(d.field_c_id_ud_organica_value, 1, 1) = 'A' THEN 'Administración Autonómica'
            WHEN SUBSTR(d.field_c_id_ud_organica_value, 1, 1) = 'E' THEN 'Administración del Estado'
            WHEN SUBSTR(d.field_c_id_ud_organica_value, 1, 1) IN ('G', 'I') THEN 'Otras instituciones'
            WHEN SUBSTR(d.field_c_id_ud_organica_value, 1, 1) = 'L' THEN 'Administración Local'
            WHEN SUBSTR(d.field_c_id_ud_organica_value, 1, 1) = 'P' THEN 'Entidad Privada'
            WHEN SUBSTR(d.field_c_id_ud_organica_value, 1, 1) = 'U' THEN 'Universidades'
            ELSE 'Desconocido'
            END AS adm_level,
            COUNT(u.uid) AS total_users
        FROM users_field_data u
        JOIN user__field_organizacion ufo ON u.uid = ufo.entity_id
        JOIN taxonomy_term__field_c_id_ud_organica d ON d.entity_id = ufo.field_organizacion_target_id
        WHERE u.status = 1
        GROUP BY adm_level
        ) s1
        GROUP BY s1.adm_level
        ORDER BY num_users DESC;
    """)
    dashboard_csv_dir =config.get('ckanext-dge-ga-report.routing_dashboard_csv','')
    date = datetime.datetime.now().strftime('%Y-%m-%d')

    public_dir = os.path.join(os.path.dirname(__file__), 'public', dashboard_csv_dir,date)

    if not os.path.exists(public_dir):
        os.makedirs(public_dir)

    file_path = os.path.join(public_dir, 'usuarios_por_nivel_administracion.csv')

    csv_content = generar_csv_desde_mysql(sql,file_path)
    


@click.group("dge_ga_report_generate_csv_admin")
def dge_ga_report_generate_csv_admin():
    """ Get's the Google auth token

    Usage: paster dge_ga_report_generate_csv <credentials_file>

    Where <credentials_file> is the file name containing the details
    for the service (obtained from https://code.google.com/apis/console).
    By default this is set to credentials.json
    """
    pass

@dge_ga_report_generate_csv_admin.command("catalogo_admin_evolucion_nivel_administracion")
def catalogo_admin_evolucion_nivel_administracion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_admin_evolucion_nivel_administracion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("catalogo_admin_organismos")
def catalogo_admin_organismos():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_admin_organismos()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("catalogo_admin_distribuciones")
def catalogo_admin_distribuciones():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_catalogo_admin_distribuciones()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)    

@dge_ga_report_generate_csv_admin.command("visitas_admin_mas_vistos")
def visitas_admin_mas_vistos():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_visitas_admin_mas_vistos()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("contenidos_admin_comentarios_recibidos")
def contenidos_admin_comentarios_recibidos():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_contenidos_admin_comentarios_recibidos()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("publicadores_admin_evolucion")
def publicadores_admin_evolucion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_publicadores_admin_evolucion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("publicadores_admin_nivel_administracion")
def publicadores_admin_nivel_administracion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_publicadores_admin_nivel_administracion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("contenido_por_tipo_administracion")
def contenido_por_tipo_administracion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_contenido_por_tipo_administracion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

@dge_ga_report_generate_csv_admin.command("disponibilidad_datos_por_estado")
def disponibilidad_datos_por_estado():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_disponibilidad_datos_por_estado()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)
@dge_ga_report_generate_csv_admin.command("usuarios_por_organismo")
def usuarios_por_organismo():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_usuarios_por_organismo()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)
@dge_ga_report_generate_csv_admin.command("usuarios_por_nivel_administracion")
def usuarios_por_nivel_administracion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_usuarios_por_nivel_administracion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)
    
@dge_ga_report_generate_csv_admin.command("publicadores_admin_nivel_administracion_forma_actualizacion")
def publicadores_admin_nivel_administracion_forma_actualizacion():
    """Genera el CSV y lo guarda en el directorio public"""
    init = datetime.datetime.now()
    log.info(f'[{init}] - Init DgeGaReportGenerateCSV command.')

    try:
        generar_csv_publicadores_admin_nivel_administracion_forma_actualizacion()
        log.info("CSV generado y guardado correctamente.")
    except Exception as e:
        log.error(f"Error al generar el CSV: {e}")
        sys.exit(1)

    end = datetime.datetime.now()
    log.info(f'[{end}] - End DgeGaReportGenerateCSV command. Ejecutado en { (end-init).total_seconds() * 1000 } ms.')
    sys.exit(0)

    