# ckanext-dge-ga-report

`ckanext-dge-ga-report` es una extensión para CKAN utilizada en la plataforma [datos.gob.es](https://datos.gob.es/) para generar información de reportes asociados a Google Analytics.

> [!TIP]
> Guía base y contexto del proyecto: https://github.com/datosgobes/datos.gob.es

## Descripción general

- Añade un plugin CKAN para generar y gestionar reportes.
- Incluye comandos `ckan` para inicialización y carga de analíticas.

## Requisitos

- Una instancia de CKAN.
- Librerías Python adicionales ([`requirements`](requirements.txt))/[`setup.py.install_requires`](setup.py)
- Requiere [`ckanext-dge-ga`](https://github.com/datosgobes/ckanext-dge-ga) y se integra con [`ckanext-dge-dashboard`](https://github.com/datosgobes/ckanext-dge-dashboard)

### Compatibilidad

Compatibilidad con versiones de CKAN:

| Versión de CKAN | ¿Compatible?                                                              |
|--------------|-----------------------------------------------------------------------------|
| 2.8          | ❌ No (requiere Python 3+)                                                   |
| 2.9          | ✅ Sí                                                                        |
| 2.10         | ❓ Desconocido                                                               |
| 2.11         | ❓ Desconocido                                                               |

## Instalación

```sh
pip install -r requirements.txt
pip install -e .
```

## Configuración

### Plugins

Activa el plugin en tu configuración de CKAN:

```ini
ckan.plugins = … dge_ga_report
```

> [!NOTE]
> La configuración específica de [datos.gob.es](https://datos.gob.es/) está documentada en:
> [Documentación extensiones CKAN](https://github.com/datosgobes/datos.gob.es/blob/master/docs/202512_datosgobes-ckan-doc_es.pdf) (sección 3.12).

### Parámetros (`ckan.ini`)

Ejemplo de parámetros utilizados en [datos.gob.es](https://datos.gob.es/) (incluye UA y GA4):

```ini
# Identificación de cuenta (usado para UA)
googleanalytics.account = ANALYTICS_ACCOUNT
googleanalytics.username = ANALYTICS_USERNAME

# Ajustes generales de la extensión
ckanext-dge-ga-report.period = monthly
ckanext-dge-ga-report.token.filepath = /ruta/a/credentials.json
ckanext-dge-ga-report.hostname = su-hostname

# Propiedades/Vistas (UA)
ckanext-dge-ga-report.prop_id_gtm = GA_PROP_ID_GTM
ckanext-dge-ga-report.prop_id = GA_PROP_ID
ckanext-dge-ga-report.view_id_gtm = GA_VIEW_ID_GTM
ckanext-dge-ga-report.view_id = GA_VIEW_ID

# Propiedades/Vistas (GA4)
ckanext-dge-ga-report.prop_id_ga4_gtm = GA_PROP_ID_GA4_GTM
ckanext-dge-ga-report.prop_id_ga4 = GA_PROP_GA4_ID
ckanext-dge-ga-report.view_id_ga4_gtm = GA_VIEW_ID_GA4_GTM
ckanext-dge-ga-report.view_id_ga4 = GA_VIEW_GA4_ID
```

Sustituir:

- `ANALYTICS_ACCOUNT`: cuenta/nombre de la cuenta de Google Analytics (UA).
- `ANALYTICS_USERNAME`: usuario de Google Analytics (si aplica a tu despliegue).
- `GA_*`: identificadores de propiedad/vista según tu configuración.

### Credenciales

Este repositorio incluye un fichero de referencia [`credentials.json.template`](./credentials.json.template) para la configuración de credenciales de Google Analytics.
Configura `ckanext-dge-ga-report.token.filepath` apuntando a un JSON válido (habitualmente credenciales de cuenta de servicio) con permisos de lectura de Analytics.

### CLI (`ckan`)

> [!NOTE]
> A partir de CKAN 2.9, el comando `ckan` sustituye al histórico *paster* usado para tareas comunes de administración de CKAN.
> Consulta la [documentación de la CLI de CKAN](https://docs.ckan.org/en/2.9/maintaining/cli.html) para más detalles.

Este repositorio expone los siguientes grupos de comandos:

- `dge_ga_report_initdb` (subcomando: `initdb`)
- `dge_ga_report_getauthtoken` (subcomando: `get_token`)
- `dge_ga_report_loadanalytics` (subcomando: `loadanalytics`)

Ejemplos (ajusta el fichero `.ini` a tu entorno):

```sh
# Crear tablas
ckan -c /etc/ckan/default/ckan.ini dge_ga_report_initdb initdb

# Verificar credenciales (fuerza inicialización del servicio)
ckan -c /etc/ckan/default/ckan.ini dge_ga_report_getauthtoken get_token
```

## Licencia

Este proyecto se distribuye bajo licencia **GNU Affero General Public License (AGPL) v3.0 o posterior**. Consulta el fichero [LICENSE](LICENSE).
