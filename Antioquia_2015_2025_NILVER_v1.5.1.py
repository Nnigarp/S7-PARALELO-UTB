#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CHALLENGE 7 - COMPUTACIÓN EN PARALELO
VERSIÓN DE RECUPERACIÓN - FEEDBACK PERSONAL + ORAL
Canalización Productor-Consumidor con Variables de Condición

Versión v1.5.0 (dataset final Antioquia 2015-2025, un único CSV):
- Mantiene la corrección personal: productores + consumidores = exactamente os.cpu_count().
- Separa cores, workers y particiones: los workers son fijos, las tareas son lotes de ZIP.
- Mantiene batching multi-ubicación: una request puede traer varias coordenadas y todo el mes.
- Añade enfriamiento coordinado y medido ante HTTP 429, sin alterar workers ni trabajo.
- Mantiene --plan-only para auditar volumen y requests antes de consultar Open-Meteo.
- Serial y paralelo usan exactamente los mismos lotes, fechas, variables y sitios.
- Se eliminan --producers y --consumers para impedir variar el conteo.
- No se introducen delays artificiales en el pipeline medido.

Autor: Nilver García Pimentel

Implementa:
- Selección por ZIP, ciudad/municipio o departamento.
- Ventana de fecha y rango horario configurable.
- Ciudad/municipio -> conjunto de ZIPs -> LAT/LON (sin reducir la ciudad a un ZIP).
- ZIP -> LAT/LON.
- API histórica de clima de Open-Meteo.
- Versión SECUENCIAL.
- Versión PARALELA Productor-Consumidor.
- Búfer acotado construido manualmente con threading.Condition.
- wait()/notify_all(), revalidación con while y cierre ordenado.
- Datos horarios: 12 variables exclusivamente relacionadas con radiación solar.
- Detección de sitios diferentes con el MISMO retorno meteorológico
  durante toda la ventana solicitada.
- Agrupación de dichos sitios sin repetir la misma serie en la tabla final.
- Trazabilidad de todos los ZIP/sitios agrupados.
- Validación secuencial == paralelo.
- Medición de tiempo y aceleración.
- Registro de ESPERA / DESPIERTA / SEÑAL / PROCESA / CIERRE.

Python 3.12+
"""

from __future__ import annotations

import argparse
import calendar
import csv
import gzip
import hashlib
import json
import logging
import os
import platform
import re
import ssl
import statistics
import sys
import threading
import time
import unicodedata
import urllib.error
import urllib.parse
import urllib.request

from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict, deque
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Iterable


# ---------------------------------------------------------------------------
# CONFIGURACIÓN DE API Y DATOS
# ---------------------------------------------------------------------------

APP_VERSION = "1.5.1"

# Política de acceso a Open-Meteo obtenida de las pruebas de carga del reto.
# La canalización mantiene sus hilos trabajadores, pero limita las solicitudes
# meteorológicas simultáneas para no saturar el servicio externo.
DEFAULT_API_CONCURRENCY = 2

# Tamaño técnico de lote para requests multi-ubicación. No modifica workers.
# 20 mantiene URLs/respuestas manejables y reduce drásticamente peticiones.
DEFAULT_BATCH_SIZE = 20

# Intentos totales máximos por solicitud lógica: 1 original + 3 reintentos.
# Ante HTTP 429 se respeta el enfriamiento indicado por el servicio; esa espera
# es externa/obligatoria por rate limit, no un retardo artificial de rendimiento.
DEFAULT_MAX_ATTEMPTS = 4
DEFAULT_429_BACKOFF_SECONDS = 65.0

ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"


# Fuente postal oficial para Colombia (4-72 / MinTIC).
CO472_MUNICIPIO_QUERY_URL = (
    "https://visor.codigopostal.gov.co/arcgis/rest/services/"
    "DivisionAdministrativa/MapServer/3/query"
)
CO472_DEPARTAMENTO_QUERY_URL = (
    "https://visor.codigopostal.gov.co/arcgis/rest/services/"
    "DivisionAdministrativa/MapServer/4/query"
)
CO472_POSTAL_QUERY_URL = (
    "https://visor.codigopostal.gov.co/arcgis/rest/services/"
    "Division_Codigo_postal/MapServer/1/query"
)
CO472_POSTAL_EXPANDED_QUERY_URL = (
    "https://visor.codigopostal.gov.co/arcgis/rest/services/"
    "Division_Codigo_postal/MapServer/0/query"
)
CO472_CACHE_PATH = Path(".challenge7_472_cache_v3.json")

# OpenStreetMap / Nominatim.
# El docente autorizó su uso para resolver códigos postales y zonas.
NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_USER_AGENT = (
    "UTB-Parallel-Computing-Challenge7/1.0 "
    "(academic exercise; OpenStreetMap geocoding)"
)
NOMINATIM_CACHE_PATH = Path(".challenge7_osm_cache_v2.json")
NOMINATIM_MIN_INTERVAL_SECONDS = 1.0


# OpenStreet / Overpass se usa para enumerar zonas postales dentro del límite
# real de un municipio cuando OSM dispone de información postal suficiente.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"
OVERPASS_URLS = (
    OVERPASS_URL,
    "https://overpass.kumi.systems/api/interpreter",
)
OVERPASS_CACHE_PATH = Path(".challenge7_overpass_cache.json")
OVERPASS_USER_AGENT = (
    "UTB-Parallel-Computing-Challenge7/1.0 "
    "(academic exercise; OpenStreetMap Overpass postcode discovery)"
)

# Alternativas verificadas para los dos municipios usados repetidamente
# durante el curso. Solo se usan cuando los servicios geográficos en línea
# no pueden enumerar el conjunto completo de códigos postales. Contienen ZIP,
# NO coordenadas inventadas; cada ZIP se resuelve de forma independiente.
KNOWN_CITY_POSTCODES: dict[str, tuple[str, ...]] = {
    "turbaco": (
        "131001",
        "131007",
        "131008",
    ),
    "cartagena": (
        "130001", "130002", "130003", "130004", "130005",
        "130006", "130007", "130008", "130009", "130010",
        "130011", "130012", "130013", "130014", "130015",
        "130017", "130018", "130019", "130027",
    ),
}

KNOWN_CITY_DEPARTMENTS: dict[str, str] = {
    "cartagena": "Bolívar",
    "turbaco": "Bolívar",
}

COLOMBIA_POSTCODE_PATTERN = re.compile(r"(?<!\d)(\d{6})(?!\d)")

# El Nominatim público exige evitar uso pesado, identificarse con User-Agent
# y no superar 1 solicitud/segundo. La resolución geográfica ocurre ANTES
# de medir secuencial vs. paralelo, por lo que este límite no altera la medición.
_NOMINATIM_LOCK = threading.Lock()
_NOMINATIM_LAST_REQUEST = 0.0

# Doce variables horarias EXCLUSIVAMENTE relacionadas con radiación solar.
# Los campos de ubicación NO se cuentan dentro de estas 12 variables.
HOURLY_VARIABLES = (
    "shortwave_radiation",
    "direct_radiation",
    "diffuse_radiation",
    "direct_normal_irradiance",
    "global_tilted_irradiance",
    "terrestrial_radiation",
    "shortwave_radiation_instant",
    "direct_radiation_instant",
    "diffuse_radiation_instant",
    "direct_normal_irradiance_instant",
    "global_tilted_irradiance_instant",
    "terrestrial_radiation_instant",
)

# Salida principal final solicitada:
# - zipcode: UN SOLO código postal representativo, conservado como STRING.
# - datetime: instante horario.
# - representative_latitude / representative_longitude: EXACTAMENTE 2 datos
#   geográficos, tomados del mismo ZIP representante.
# - después aparecen EXACTAMENTE 12 variables de radiación solar.
#
# No se exporta group_id ni una lista gigante de ZIP en una celda.
# La agrupación se decide con TODA la serie temporal (datetime + 12 variables
# solares, igualdad exacta). La trazabilidad de los ZIP agrupados se imprime
# claramente en consola mediante print_grouping_summary().
FINAL_CSV_FIELDS = (
    "zipcode",
    "datetime",
    "representative_latitude",
    "representative_longitude",
    *HOURLY_VARIABLES,
)

GROUP_CSV_FIELDS = (
    "group_id",
    "representative_zip",
    "representative_site",
    "representative_latitude",
    "representative_longitude",
    "coordinate_source",
    "minlon",
    "minlat",
    "maxlon",
    "maxlat",
    "member_count",
    "grouped_zip_codes",
    "grouped_sites",
    "grouped_coordinates",
    "same_weather_series",
)

# Catálogo mínimo incorporado para que el archivo sea autocontenido.
# No pretende ser una base postal completa de Colombia. Para expandir una
# ciudad/departamento a muchos ZIP, se puede pasar --catalog locations.csv.
#
# Formato de cada tupla:
# zip, lat, lon, ciudad/municipio, departamento, nombre_visible, código_país
BUILTIN_CATALOG_RAW = (
    ("130001", 10.400833, -75.558056, "Cartagena", "Bolívar",
     "Cartagena, Bolívar", "CO"),
    ("110111", 4.695219, -74.030932, "Bogotá", "Bogotá D.C.",
     "Bogotá D.C.", "CO"),
    ("050015", 6.244203, -75.581211, "Medellín", "Antioquia",
     "Medellín, Antioquia", "CO"),
    ("760001", 3.451647, -76.531985, "Cali", "Valle del Cauca",
     "Cali, Valle del Cauca", "CO"),
    ("080002", 10.987760, -74.788900, "Barranquilla", "Atlántico",
     "Barranquilla, Atlántico", "CO"),
)


# ---------------------------------------------------------------------------
# ERRORES Y CLASES DE DATOS
# ---------------------------------------------------------------------------

class ChallengeError(Exception):
    """Error controlado del Challenge."""


class LocationResolutionError(ChallengeError):
    """No fue posible resolver una selección geográfica."""


class APIRequestError(ChallengeError):
    """Falló una solicitud HTTP/API con metadatos opcionales de rate limit."""

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        retry_after: float | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.retry_after = retry_after


class DataValidationError(ChallengeError):
    """La API devolvió datos incompletos o inconsistentes."""


@dataclass(frozen=True)
class Location:
    zip_code: str
    latitude: float
    longitude: float
    city: str
    department: str
    name: str
    country_code: str = "CO"
    coordinate_source: str = "catalog"
    minlon: float | None = None
    minlat: float | None = None
    maxlon: float | None = None
    maxlat: float | None = None

    def __post_init__(self) -> None:
        # El código postal es un IDENTIFICADOR, nunca una cantidad numérica.
        # Para Colombia se conserva/recupera siempre el formato de 6 dígitos.
        cc = str(self.country_code or "CO").strip().upper()
        raw_zip = str(self.zip_code).strip()

        if cc == "CO":
            if not raw_zip.isdigit() or len(raw_zip) > 6:
                raise ValueError(
                    f"Código postal colombiano inválido: {self.zip_code!r}. "
                    "Debe contener únicamente dígitos y ocupar 6 posiciones."
                )
            raw_zip = raw_zip.zfill(6)
            if not re.fullmatch(r"\d{6}", raw_zip):
                raise ValueError(
                    f"Código postal colombiano inválido tras normalización: {raw_zip!r}."
                )

        object.__setattr__(self, "zip_code", raw_zip)
        object.__setattr__(self, "country_code", cc)

    @property
    def identity(self) -> tuple[str, float, float]:
        return (
            self.zip_code.strip().upper(),
            round(float(self.latitude), 7),
            round(float(self.longitude), 7),
        )


@dataclass(frozen=True)
class LocationBatch:
    """Partición lógica: un lote de ubicaciones para una request multi-location."""

    batch_id: str
    locations: tuple[Location, ...]

    @property
    def size(self) -> int:
        return len(self.locations)

    @property
    def label(self) -> str:
        return f"{self.batch_id}({self.size} ZIP)"


@dataclass(frozen=True)
class PipelineItem:
    location: Location
    payload: dict[str, Any]


@dataclass(frozen=True)
class SiteSeries:
    location: Location
    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class WeatherGroup:
    group_id: str
    representative: Location
    members: tuple[Location, ...]
    rows: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class ExecutionResult:
    """Resultado completo de una modalidad en una repetición."""

    groups: list[WeatherGroup]
    rows: list[dict[str, Any]]
    duplicate_inputs: int
    elapsed: float
    errors: list[str]
    sync_stats: dict[str, int] | None = None
    request_stats: dict[str, int] | None = None


# ---------------------------------------------------------------------------
# FUNCIONES AUXILIARES GENERALES
# ---------------------------------------------------------------------------

def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value.strip())
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.replace(".", " ").replace("_", " ")
    return " ".join(text.lower().split())


def join_semicolon(values: Iterable[str]) -> str:
    return ";".join(str(v) for v in values)


def coordinate_text(location: Location) -> str:
    return f"{location.latitude:.6f},{location.longitude:.6f}"


def print_separator(title: str) -> None:
    print()
    print("=" * 76)
    print(title)
    print("=" * 76)


def parse_zip_spec(spec: str) -> tuple[str, str | None]:
    """
    Acepta:
      130001
      130001:CO
      75001:FR
    """
    text = spec.strip()
    if not text:
        raise argparse.ArgumentTypeError("ZIP vacío")

    if ":" not in text:
        return text, None

    zip_code, cc = text.rsplit(":", 1)
    zip_code = zip_code.strip()
    cc = cc.strip().upper()

    if not zip_code or len(cc) != 2 or not cc.isalpha():
        raise argparse.ArgumentTypeError(
            f"Formato inválido '{spec}'. Use ZIP o ZIP:CC, por ejemplo 130001:CO."
        )

    return zip_code, cc


def fixed_worker_plan(logical_cores: int) -> tuple[int, int]:
    """
    Regla literal del docente para el modo paralelo:

    - usar TODOS los núcleos lógicos disponibles como presupuesto de workers;
    - no reducir el número de workers según la cantidad de trabajos;
    - no permitir que el usuario varíe productores/consumidores por CLI.

    El presupuesto se reparte de forma balanceada entre productores y
    consumidores. La suma SIEMPRE es exactamente ``logical_cores``.
    """
    if logical_cores < 2:
        raise ChallengeError(
            "El patrón Productor-Consumidor requiere al menos 2 núcleos lógicos "
            "para mantener un productor y un consumidor usando exactamente el "
            "presupuesto detectado."
        )

    producer_count = (logical_cores + 1) // 2
    consumer_count = logical_cores - producer_count

    if producer_count < 1 or consumer_count < 1:
        raise RuntimeError("Plan de workers inválido.")
    if producer_count + consumer_count != logical_cores:
        raise RuntimeError("El plan de workers no usa exactamente todos los cores.")

    return producer_count, consumer_count



def parse_hour(value: str) -> int:
    """
    Acepta una hora completa:
      8
      08
      08:00

    Open-Meteo devuelve resolución horaria; por eso no se aceptan minutos
    distintos de :00.
    """
    raw = value.strip()

    match = re.fullmatch(r"(\d{1,2})(?::(\d{2}))?", raw)
    if not match:
        raise argparse.ArgumentTypeError(
            "Hora inválida. Use HH o HH:00, por ejemplo 8, 08 o 08:00."
        )

    hour = int(match.group(1))
    minute_text = match.group(2)
    minute = int(minute_text) if minute_text is not None else 0

    if not 0 <= hour <= 23 or minute != 0:
        raise argparse.ArgumentTypeError(
            "La hora debe estar entre 00:00 y 23:00 y usar minutos :00."
        )

    return hour


def validate_hour_window(
    start_hour: int | None,
    end_hour: int | None,
) -> None:
    if (start_hour is None) != (end_hour is None):
        raise argparse.ArgumentTypeError(
            "--start-hour y --end-hour deben usarse juntos."
        )

    if start_hour is None:
        return

    if start_hour > end_hour:
        raise argparse.ArgumentTypeError(
            "--start-hour no puede ser posterior a --end-hour "
            "en una ventana diaria."
        )


def format_hour_window(
    start_hour: int | None,
    end_hour: int | None,
) -> str:
    if start_hour is None:
        return "00:00 -> 23:00 (todos los registros horarios)"
    return f"{start_hour:02d}:00 -> {end_hour:02d}:00"

def validate_dates(start_text: str, end_text: str) -> None:
    try:
        start = date.fromisoformat(start_text)
        end = date.fromisoformat(end_text)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            "Las fechas deben estar en formato YYYY-MM-DD."
        ) from exc

    if start > end:
        raise argparse.ArgumentTypeError(
            "--start no puede ser posterior a --end."
        )

    if start.year < 1940:
        raise argparse.ArgumentTypeError(
            "La Historical Weather API usa datos históricos desde 1940."
        )

    if end > date.today():
        raise argparse.ArgumentTypeError(
            "--end no puede estar en el futuro para Historical Weather API."
        )


# ---------------------------------------------------------------------------
# SSL / HTTP
# ---------------------------------------------------------------------------

def _load_certifi_context() -> ssl.SSLContext | None:
    try:
        import certifi  # type: ignore
    except ImportError:
        return None
    return ssl.create_default_context(cafile=certifi.where())


def _is_certificate_error(exc: BaseException) -> bool:
    if isinstance(exc, ssl.SSLCertVerificationError):
        return True

    reason = getattr(exc, "reason", None)
    if isinstance(reason, ssl.SSLCertVerificationError):
        return True

    text = str(reason if reason is not None else exc).upper()
    return (
        "CERTIFICATE_VERIFY_FAILED" in text
        or "CERTIFICATE HAS EXPIRED" in text
    )


def _ssl_attempts(ssl_mode: str) -> list[tuple[str, ssl.SSLContext]]:
    if ssl_mode == "system":
        return [("system", ssl.create_default_context())]

    if ssl_mode == "certifi":
        ctx = _load_certifi_context()
        if ctx is None:
            raise APIRequestError(
                "--ssl-mode certifi requiere certifi. Ejecute: "
                "python -m pip install --upgrade certifi"
            )
        return [("certifi", ctx)]

    if ssl_mode == "insecure":
        logging.warning(
            "Verificación SSL DESACTIVADA. Use --ssl-mode insecure solo para "
            "diagnóstico, NO para la entrega final."
        )
        return [("insecure", ssl._create_unverified_context())]

    attempts = [("system", ssl.create_default_context())]
    certifi_ctx = _load_certifi_context()
    if certifi_ctx is not None:
        attempts.append(("certifi", certifi_ctx))
    return attempts


def _http_get_json_value(
    url: str,
    timeout: float,
    ssl_mode: str,
) -> Any:
    """GET JSON común; admite objeto o lista para requests multi-location."""
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": "UTB-Parallel-Computing-Challenge7/2.0",
            "Accept": "application/json",
        },
    )

    certificate_errors: list[str] = []

    for index, (label, context) in enumerate(_ssl_attempts(ssl_mode)):
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout,
                context=context,
            ) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                raw = response.read().decode(charset)
            if index > 0:
                logging.info("TLS OK usando bundle %s", label)
            break

        except urllib.error.HTTPError as exc:
            try:
                body = exc.read().decode("utf-8", errors="replace")
            except Exception:
                body = ""
            retry_after: float | None = None
            try:
                raw_retry_after = exc.headers.get("Retry-After")
                if raw_retry_after is not None:
                    retry_after = max(0.0, float(raw_retry_after))
            except (TypeError, ValueError):
                retry_after = None

            raise APIRequestError(
                f"HTTP {exc.code}: {body[:200]}",
                status_code=exc.code,
                retry_after=retry_after,
            ) from exc

        except (urllib.error.URLError, ssl.SSLCertVerificationError) as exc:
            if _is_certificate_error(exc):
                certificate_errors.append(f"{label}: {exc}")
                continue
            reason = getattr(exc, "reason", exc)
            raise APIRequestError(f"Error de red: {reason}") from exc

        except TimeoutError as exc:
            raise APIRequestError(
                f"Timeout después de {timeout:.1f}s"
            ) from exc
    else:
        raise APIRequestError(
            "Falló la validación SSL. Actualice certifi y use "
            "--ssl-mode certifi. Detalle: "
            + " | ".join(certificate_errors)
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise APIRequestError("La API devolvió JSON inválido.") from exc

    if isinstance(data, dict) and data.get("error"):
        raise APIRequestError(str(data.get("reason", "Error de API")))

    return data


def http_get_json(
    url: str,
    timeout: float,
    ssl_mode: str,
) -> dict[str, Any]:
    data = _http_get_json_value(url, timeout, ssl_mode)
    if not isinstance(data, dict):
        raise APIRequestError("La API devolvió una estructura inesperada.")
    return data



def http_post_form_json(
    url: str,
    params: dict[str, Any],
    timeout: float,
    ssl_mode: str,
    user_agent: str = "UTB-Parallel-Computing-Challenge7/2.0",
) -> dict[str, Any]:
    body = urllib.parse.urlencode(params).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/json",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    certificate_errors: list[str] = []

    for label, context in _ssl_attempts(ssl_mode):
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout,
                context=context,
            ) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                raw = response.read().decode(charset)
            break
        except urllib.error.HTTPError as exc:
            try:
                detail = exc.read().decode("utf-8", errors="replace")
            except Exception:
                detail = ""
            raise APIRequestError(
                f"HTTP {exc.code} consultando servicio geográfico: "
                f"{detail[:250]}"
            ) from exc
        except (urllib.error.URLError, ssl.SSLCertVerificationError) as exc:
            if _is_certificate_error(exc):
                certificate_errors.append(f"{label}: {exc}")
                continue
            reason = getattr(exc, "reason", exc)
            raise APIRequestError(
                f"Error de red consultando servicio geográfico: {reason}"
            ) from exc
        except TimeoutError as exc:
            raise APIRequestError(
                f"Tiempo de espera agotado después de {timeout:.1f}s"
            ) from exc
    else:
        raise APIRequestError(
            "Falló la validación SSL del servicio geográfico. "
            + " | ".join(certificate_errors)
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise APIRequestError(
            "El servicio geográfico devolvió JSON inválido."
        ) from exc

    if not isinstance(data, dict):
        raise APIRequestError(
            "El servicio geográfico devolvió una estructura inesperada."
        )

    if "error" in data:
        raise APIRequestError(
            f"Servicio geográfico: {data['error']}"
        )

    return data



# ---------------------------------------------------------------------------
# FUNCIONES AUXILIARES DE OPENSTREETMAP / NOMINATIM
# ---------------------------------------------------------------------------

def _load_nominatim_cache() -> dict[str, Any]:
    try:
        if NOMINATIM_CACHE_PATH.exists():
            data = json.loads(
                NOMINATIM_CACHE_PATH.read_text(encoding="utf-8")
            )
            if isinstance(data, dict):
                return data
    except Exception as exc:
        logging.warning("No se pudo leer cache OSM: %s", exc)
    return {}


def _save_nominatim_cache(cache: dict[str, Any]) -> None:
    try:
        NOMINATIM_CACHE_PATH.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logging.warning("No se pudo guardar cache OSM: %s", exc)


def _http_get_json_list(
    url: str,
    timeout: float,
    ssl_mode: str,
    user_agent: str,
) -> list[dict[str, Any]]:
    request = urllib.request.Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "application/json",
        },
    )

    certificate_errors: list[str] = []

    for label, context in _ssl_attempts(ssl_mode):
        try:
            with urllib.request.urlopen(
                request,
                timeout=timeout,
                context=context,
            ) as response:
                charset = response.headers.get_content_charset() or "utf-8"
                raw = response.read().decode(charset)
            break
        except urllib.error.HTTPError as exc:
            raise APIRequestError(
                f"OpenStreet/Nominatim HTTP {exc.code}"
            ) from exc
        except (urllib.error.URLError, ssl.SSLCertVerificationError) as exc:
            if _is_certificate_error(exc):
                certificate_errors.append(f"{label}: {exc}")
                continue
            reason = getattr(exc, "reason", exc)
            raise APIRequestError(
                f"Error de red consultando OpenStreet/Nominatim: {reason}"
            ) from exc
        except TimeoutError as exc:
            raise APIRequestError(
                f"Timeout OpenStreet/Nominatim después de {timeout:.1f}s"
            ) from exc
    else:
        raise APIRequestError(
            "Falló SSL con OpenStreet/Nominatim. "
            + " | ".join(certificate_errors)
        )

    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise APIRequestError(
            "OpenStreet/Nominatim devolvió JSON inválido."
        ) from exc

    if not isinstance(data, list):
        raise APIRequestError(
            "OpenStreet/Nominatim devolvió una estructura inesperada."
        )

    return [item for item in data if isinstance(item, dict)]


def nominatim_search(
    params: dict[str, Any],
    timeout: float,
    ssl_mode: str,
) -> list[dict[str, Any]]:
    """
    Consulta Nominatim respetando la política del servidor público:
    - User-Agent identificable.
    - Máximo 1 solicitud/segundo.
    - Cache local para no repetir consultas iguales.
    """
    global _NOMINATIM_LAST_REQUEST

    normalized_params = {
        **params,
        "format": "jsonv2",
        "addressdetails": 1,
    }
    query = urllib.parse.urlencode(
        sorted((str(k), str(v)) for k, v in normalized_params.items())
    )
    cache_key = query

    with _NOMINATIM_LOCK:
        cache = _load_nominatim_cache()
        cached = cache.get(cache_key)
        if isinstance(cached, list):
            logging.info("[OSM] cache HIT")
            return [
                item for item in cached
                if isinstance(item, dict)
            ]

        elapsed = time.monotonic() - _NOMINATIM_LAST_REQUEST
        remaining = NOMINATIM_MIN_INTERVAL_SECONDS - elapsed
        if remaining > 0:
            # No es un retardo artificial del experimento: es cumplimiento
            # de la política de la API de Nominatim y ocurre antes de la medición.
            threading.Event().wait(remaining)

        url = f"{NOMINATIM_URL}?{urllib.parse.urlencode(normalized_params)}"
        logging.info("[OSM] Nominatim request: %s", params)

        data = _http_get_json_list(
            url,
            timeout,
            ssl_mode,
            NOMINATIM_USER_AGENT,
        )
        _NOMINATIM_LAST_REQUEST = time.monotonic()

        cache[cache_key] = data
        _save_nominatim_cache(cache)

        return data


def _osm_result_to_location(
    result: dict[str, Any],
    requested_zip: str,
    fallback_country: str | None,
) -> Location:
    try:
        result_lat = float(result["lat"])
        result_lon = float(result["lon"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LocationResolutionError(
            f"OpenStreet no devolvió LAT/LON válidos para ZIP {requested_zip}."
        ) from exc

    address = result.get("address")
    if not isinstance(address, dict):
        address = {}

    bbox = result.get("boundingbox")
    minlon = minlat = maxlon = maxlat = None

    # El JSON de Nominatim devuelve la caja delimitadora:
    # [latitud_sur, latitud_norte, longitud_oeste, longitud_este].
    if isinstance(bbox, list) and len(bbox) == 4:
        try:
            minlat = float(bbox[0])
            maxlat = float(bbox[1])
            minlon = float(bbox[2])
            maxlon = float(bbox[3])
        except (TypeError, ValueError):
            minlon = minlat = maxlon = maxlat = None

    city = str(
        address.get("city")
        or address.get("town")
        or address.get("municipality")
        or address.get("village")
        or result.get("name")
        or requested_zip
    )
    department = str(
        address.get("state")
        or address.get("region")
        or address.get("county")
        or ""
    )
    cc = str(
        address.get("country_code")
        or fallback_country
        or "CO"
    ).upper()

    display_name = str(
        result.get("display_name")
        or f"{city}, {department}"
    )

    return Location(
        zip_code=requested_zip,
        latitude=result_lat,
        longitude=result_lon,
        city=city,
        department=department,
        name=display_name,
        country_code=cc,
        coordinate_source="OpenStreetMap/Nominatim",
        minlon=minlon,
        minlat=minlat,
        maxlon=maxlon,
        maxlat=maxlat,
    )


def resolve_zip_openstreet(
    zip_code: str,
    country_code: str | None,
    timeout: float,
    ssl_mode: str,
) -> Location:
    """
    Resolución ZIP -> LAT/LON + bounding box usando OpenStreet/Nominatim.

    Primero usa búsqueda estructurada por postalcode, como recomienda
    Nominatim. Si no hay resultado, intenta búsqueda libre equivalente
    a la búsqueda del enlace que compartió el docente.
    """
    cc = (country_code or "CO").lower()

    structured = nominatim_search(
        {
            "postalcode": zip_code,
            "countrycodes": cc,
            "limit": 5,
        },
        timeout,
        ssl_mode,
    )

    results = structured

    if not results:
        results = nominatim_search(
            {
                "q": f"{zip_code}, Colombia" if cc == "co" else zip_code,
                "countrycodes": cc,
                "limit": 5,
            },
            timeout,
            ssl_mode,
        )

    if not results:
        raise LocationResolutionError(
            f"OpenStreet/Nominatim no encontró ZIP '{zip_code}:{cc.upper()}'."
        )

    # Preferir el resultado cuyo código postal coincida exactamente.
    wanted = zip_code.replace(" ", "").upper()
    selected = results[0]

    for candidate in results:
        address = candidate.get("address")
        if isinstance(address, dict):
            postcode = str(address.get("postcode") or "")
            if postcode.replace(" ", "").upper() == wanted:
                selected = candidate
                break

    return _osm_result_to_location(
        selected,
        requested_zip=zip_code,
        fallback_country=cc,
    )



def _address_city_values(result: dict[str, Any]) -> set[str]:
    address = result.get("address")
    if not isinstance(address, dict):
        address = {}

    values = {
        normalize_text(str(value))
        for key in ("city", "town", "municipality", "village", "county")
        for value in [address.get(key)]
        if value
    }
    return values


def _address_department_value(result: dict[str, Any]) -> str:
    address = result.get("address")
    if not isinstance(address, dict):
        return ""

    return normalize_text(
        str(
            address.get("state")
            or address.get("region")
            or address.get("county")
            or ""
        )
    )


def _address_postcode_value(result: dict[str, Any]) -> str:
    address = result.get("address")
    if not isinstance(address, dict):
        return ""

    return str(address.get("postcode") or "").replace(" ", "").upper()


def resolve_zip_openstreet_in_city(
    zip_code: str,
    city_name: str,
    department: str | None,
    country_code: str | None,
    timeout: float,
    ssl_mode: str,
) -> Location:
    """
    Resolución estricta de ZIP con validación de municipio.

    A diferencia de la búsqueda ZIP genérica, esta consulta vincula código
    postal + ciudad + departamento. Se rechaza un resultado si pertenece a
    otro departamento o municipio. Esto evita falsos positivos, por ejemplo
    que un ZIP de Cartagena termine resuelto en Huila o Córdoba.
    """
    cc = (country_code or "CO").lower()
    wanted_zip = zip_code.replace(" ", "").upper()
    wanted_city = normalize_text(city_name)
    wanted_department = normalize_text(department or "")

    params: dict[str, Any] = {
        "postalcode": zip_code,
        "city": city_name,
        "countrycodes": cc,
        "limit": 10,
    }
    if department:
        params["state"] = department

    results = nominatim_search(
        params,
        timeout,
        ssl_mode,
    )

    # La alternativa de texto libre sigue restringida por municipio + departamento.
    if not results:
        place = ", ".join(
            part
            for part in (zip_code, city_name, department, "Colombia")
            if part
        )
        results = nominatim_search(
            {
                "q": place,
                "countrycodes": cc,
                "limit": 10,
            },
            timeout,
            ssl_mode,
        )

    valid: list[dict[str, Any]] = []

    for candidate in results:
        candidate_zip = _address_postcode_value(candidate)
        candidate_cities = _address_city_values(candidate)
        candidate_department = _address_department_value(candidate)

        # El ZIP debe coincidir exactamente cuando Nominatim lo proporciona.
        if candidate_zip and candidate_zip != wanted_zip:
            continue

        # El departamento es una restricción estricta cuando se conoce.
        if (
            wanted_department
            and candidate_department
            and candidate_department != wanted_department
        ):
            continue

        # La ciudad también es una restricción estricta cuando los metadatos la proporcionan.
        if candidate_cities and wanted_city not in candidate_cities:
            continue

        valid.append(candidate)

    if not valid:
        raise LocationResolutionError(
            f"OpenStreet no encontró una coincidencia válida para ZIP "
            f"{zip_code} dentro de {city_name}"
            + (f", {department}" if department else "")
            + "."
        )

    location = _osm_result_to_location(
        valid[0],
        requested_zip=zip_code,
        fallback_country=cc,
    )

    # Verificación defensiva final después de la conversión.
    if (
        wanted_department
        and normalize_text(location.department)
        and normalize_text(location.department) != wanted_department
    ):
        raise LocationResolutionError(
            f"ZIP {zip_code} fue rechazado: OpenStreet lo ubicó en "
            f"{location.department}, no en {department}."
        )

    return location


def _expected_city_department(
    city_name: str,
    catalog: list[Location],
) -> str:
    key = normalize_text(city_name)

    known = KNOWN_CITY_DEPARTMENTS.get(key)
    if known:
        return known

    for location in catalog:
        if normalize_text(location.city) == key and location.department:
            return location.department

    return ""


def _location_matches_expected_department(
    location: Location,
    expected_department: str,
) -> bool:
    if not expected_department:
        return True

    if not location.department:
        return False

    return (
        normalize_text(location.department)
        == normalize_text(expected_department)
    )

def bbox_text(location: Location) -> str:
    if None in (
        location.minlon,
        location.minlat,
        location.maxlon,
        location.maxlat,
    ):
        return "bbox=N/A"

    return (
        f"minlon={location.minlon:.8f}, "
        f"minlat={location.minlat:.8f}, "
        f"maxlon={location.maxlon:.8f}, "
        f"maxlat={location.maxlat:.8f}"
    )



# ---------------------------------------------------------------------------
# OPENSTREET / OVERPASS: CIUDAD -> CONJUNTO DE CÓDIGOS POSTALES
# ---------------------------------------------------------------------------

def _load_overpass_cache() -> dict[str, Any]:
    try:
        if OVERPASS_CACHE_PATH.exists():
            data = json.loads(
                OVERPASS_CACHE_PATH.read_text(encoding="utf-8")
            )
            if isinstance(data, dict):
                return data
    except Exception as exc:
        logging.warning("No se pudo leer cache Overpass: %s", exc)
    return {}


def _save_overpass_cache(cache: dict[str, Any]) -> None:
    try:
        OVERPASS_CACHE_PATH.write_text(
            json.dumps(cache, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    except Exception as exc:
        logging.warning("No se pudo guardar cache Overpass: %s", exc)


def _postcode_values(raw_value: Any) -> list[str]:
    if raw_value is None:
        return []
    return COLOMBIA_POSTCODE_PATTERN.findall(str(raw_value))


def _select_city_nominatim_result(
    city_name: str,
    timeout: float,
    ssl_mode: str,
) -> dict[str, Any] | None:
    requested = normalize_text(city_name)

    # Primero se intenta una búsqueda estructurada de la ciudad.
    try:
        results = nominatim_search(
            {
                "city": city_name,
                "countrycodes": "co",
                "limit": 10,
            },
            timeout,
            ssl_mode,
        )
    except ChallengeError as exc:
        logging.warning(
            "[OSM] No se pudo buscar municipio '%s': %s",
            city_name,
            exc,
        )
        return None

    # Si no hay resultado, se usa como alternativa una búsqueda de texto libre.
    if not results:
        try:
            results = nominatim_search(
                {
                    "q": f"{city_name}, Colombia",
                    "countrycodes": "co",
                    "limit": 10,
                },
                timeout,
                ssl_mode,
            )
        except ChallengeError:
            return None

    if not results:
        return None

    best = None
    for candidate in results:
        address = candidate.get("address")
        if not isinstance(address, dict):
            address = {}

        names = [
            candidate.get("name"),
            address.get("city"),
            address.get("town"),
            address.get("municipality"),
            address.get("village"),
        ]
        if any(
            normalize_text(str(name)) == requested
            for name in names
            if name
        ):
            return candidate

        if best is None:
            best = candidate

    return best


def _overpass_area_id(city_result: dict[str, Any]) -> int | None:
    try:
        osm_id = int(city_result["osm_id"])
    except (KeyError, TypeError, ValueError):
        return None

    osm_type = str(city_result.get("osm_type") or "").lower()

    # Identificadores de área derivados por Overpass:
    # relación -> 3600000000 + id de relación
    # vía      -> 2400000000 + id de vía
    if osm_type == "relation":
        return 3_600_000_000 + osm_id
    if osm_type == "way":
        return 2_400_000_000 + osm_id
    return None


def _element_center(element: dict[str, Any]) -> tuple[float, float] | None:
    try:
        if "lat" in element and "lon" in element:
            return float(element["lat"]), float(element["lon"])
    except (TypeError, ValueError):
        pass

    center = element.get("center")
    if isinstance(center, dict):
        try:
            return float(center["lat"]), float(center["lon"])
        except (KeyError, TypeError, ValueError):
            return None

    return None


def discover_city_postcodes_openstreet(
    city_name: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Busca el municipio en Nominatim y, cuando existe un área administrativa,
    usa Overpass para enumerar códigos postales realmente mapeados DENTRO de
    esa área.

    No convierte el centro de la ciudad en un único ZIP.
    """
    city_result = _select_city_nominatim_result(
        city_name,
        timeout,
        ssl_mode,
    )
    if city_result is None:
        return []

    area_id = _overpass_area_id(city_result)
    if area_id is None:
        logging.info(
            "[OSM] %s no tiene área derivable para enumerar postcodes.",
            city_name,
        )
        return []

    address = city_result.get("address")
    if not isinstance(address, dict):
        address = {}

    department = str(
        address.get("state")
        or address.get("region")
        or address.get("county")
        or ""
    )
    country_code = str(address.get("country_code") or "CO").upper()

    query = f"""
[out:json][timeout:30];
area({area_id})->.searchArea;
(
  rel(area.searchArea)["boundary"="postal_code"]["postal_code"];
  way(area.searchArea)["boundary"="postal_code"]["postal_code"];
  rel(area.searchArea)["postal_code"];
  way(area.searchArea)["postal_code"];
  node(area.searchArea)["postal_code"];
  node(area.searchArea)["addr:postcode"];
  way(area.searchArea)["addr:postcode"];
  rel(area.searchArea)["addr:postcode"];
);
out center tags;
""".strip()

    cache_key = f"area={area_id}|city={normalize_text(city_name)}"
    cache = _load_overpass_cache()

    data = cache.get(cache_key)
    if not isinstance(data, dict):
        url = (
            f"{OVERPASS_URL}?"
            + urllib.parse.urlencode({"data": query})
        )
        try:
            data = http_get_json(
                url,
                max(timeout, 35.0),
                ssl_mode,
            )
        except ChallengeError as exc:
            logging.warning(
                "[OSM] Overpass no pudo enumerar ZIPs de %s: %s",
                city_name,
                exc,
            )
            return []

        cache[cache_key] = data
        _save_overpass_cache(cache)

    elements = data.get("elements")
    if not isinstance(elements, list):
        return []

    # código postal -> (prioridad, ubicación)
    # Un número de prioridad menor es preferible.
    by_postcode: dict[str, tuple[int, Location]] = {}

    for element in elements:
        if not isinstance(element, dict):
            continue

        tags = element.get("tags")
        if not isinstance(tags, dict):
            tags = {}

        candidates: list[tuple[int, str]] = []

        for code in _postcode_values(tags.get("postal_code")):
            priority = 0 if tags.get("boundary") == "postal_code" else 1
            candidates.append((priority, code))

        for code in _postcode_values(tags.get("addr:postcode")):
            candidates.append((2, code))

        if not candidates:
            continue

        center = _element_center(element)
        if center is None:
            # Aún conocemos el código postal; se resolverá después mediante
            # Nominatim/Open-Meteo si es necesario.
            lat = lon = None
        else:
            lat, lon = center

        for priority, postcode in candidates:
            if postcode in by_postcode and by_postcode[postcode][0] <= priority:
                continue

            if lat is None or lon is None:
                # No se emite un marcador provisional porque las coordenadas son obligatorias.
                continue

            by_postcode[postcode] = (
                priority,
                Location(
                    zip_code=postcode,
                    latitude=lat,
                    longitude=lon,
                    city=city_name,
                    department=department,
                    name=f"{city_name} - ZIP {postcode}",
                    country_code=country_code,
                    coordinate_source="OpenStreetMap/Overpass",
                ),
            )

    result = [value[1] for value in by_postcode.values()]
    result.sort(key=lambda loc: loc.zip_code)

    if result:
        logging.info(
            "[OSM] %s -> %d postcode(s) discovered by Overpass: %s",
            city_name,
            len(result),
            ", ".join(loc.zip_code for loc in result),
        )

    return result


# ---------------------------------------------------------------------------
# CATÁLOGO Y RESOLUCIÓN DE UBICACIONES
# ---------------------------------------------------------------------------

def builtin_catalog() -> list[Location]:
    return [
        Location(
            zip_code=z,
            latitude=lat,
            longitude=lon,
            city=city,
            department=department,
            name=name,
            country_code=cc,
        )
        for z, lat, lon, city, department, name, cc in BUILTIN_CATALOG_RAW
    ]


def load_external_catalog(path: Path) -> list[Location]:
    """
    CSV opcional para cubrir muchos ZIPs/municipios/departamentos.

    Columnas requeridas:
      zip_code, latitude, longitude, city, department

    Opcionales:
      name, country_code

    Ejemplo:
      130001,10.40,-75.55,Cartagena,Bolívar,Cartagena Zona 1,CO
    """
    if not path.exists():
        raise LocationResolutionError(
            f"No existe el catálogo: {path}"
        )

    locations: list[Location] = []

    with path.open("r", newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        required = {
            "zip_code",
            "latitude",
            "longitude",
            "city",
            "department",
        }

        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise LocationResolutionError(
                "El catálogo debe contener: "
                "zip_code,latitude,longitude,city,department"
            )

        for line_number, row in enumerate(reader, start=2):
            try:
                zip_code = str(row["zip_code"]).strip()
                lat = float(row["latitude"])
                lon = float(row["longitude"])
                city = str(row["city"]).strip()
                department = str(row["department"]).strip()
                name = str(row.get("name") or "").strip()
                cc = str(row.get("country_code") or "CO").strip().upper()
            except (TypeError, ValueError) as exc:
                raise LocationResolutionError(
                    f"Fila inválida en catálogo, línea {line_number}: {exc}"
                ) from exc

            if not zip_code or not city or not department:
                raise LocationResolutionError(
                    f"Fila incompleta en catálogo, línea {line_number}."
                )

            locations.append(
                Location(
                    zip_code=zip_code,
                    latitude=lat,
                    longitude=lon,
                    city=city,
                    department=department,
                    name=name or f"{city} - ZIP {zip_code}",
                    country_code=cc or "CO",
                )
            )

    return locations


def merge_catalogs(
    builtins: list[Location],
    externals: list[Location],
) -> list[Location]:
    # El catálogo externo prevalece por ZIP, no por coordenada exacta.
    # Un mismo código postal no debe duplicarse porque dos fuentes hayan
    # elegido centros ligeramente diferentes.
    merged: dict[tuple[str, str], Location] = {
        (loc.country_code.upper(), loc.zip_code.strip().upper()): loc
        for loc in builtins
    }

    for loc in externals:
        key = (
            loc.country_code.upper(),
            loc.zip_code.strip().upper(),
        )
        merged[key] = loc

    return list(merged.values())


def find_static_zip(
    catalog: list[Location],
    zip_code: str,
    country_code: str | None,
) -> Location | None:
    wanted_zip = zip_code.replace(" ", "").upper()
    wanted_cc = country_code.upper() if country_code else None

    for location in catalog:
        candidate_zip = location.zip_code.replace(" ", "").upper()
        if candidate_zip != wanted_zip:
            continue
        if wanted_cc and location.country_code.upper() != wanted_cc:
            continue
        return location

    return None


def resolve_zip_open_meteo(
    zip_code: str,
    country_code: str | None,
    timeout: float,
    ssl_mode: str,
) -> Location:
    params: dict[str, Any] = {
        "name": zip_code,
        "count": 10,
        "language": "es",
        "format": "json",
    }
    if country_code:
        params["countryCode"] = country_code

    url = f"{GEOCODING_URL}?{urllib.parse.urlencode(params)}"
    data = http_get_json(url, timeout, ssl_mode)

    results = data.get("results")
    if not isinstance(results, list) or not results:
        raise LocationResolutionError(
            f"No se pudo resolver ZIP '{zip_code}'."
        )

    selected = results[0]
    wanted = zip_code.replace(" ", "").upper()

    for candidate in results:
        postcodes = candidate.get("postcodes") or []
        if isinstance(postcodes, list):
            normalized = {
                str(x).replace(" ", "").upper()
                for x in postcodes
            }
            if wanted in normalized:
                selected = candidate
                break

    try:
        lat = float(selected["latitude"])
        lon = float(selected["longitude"])
    except (KeyError, TypeError, ValueError) as exc:
        raise LocationResolutionError(
            f"ZIP '{zip_code}' sin LAT/LON válidos."
        ) from exc

    city = str(selected.get("name") or zip_code)
    department = str(selected.get("admin1") or "")
    country = str(selected.get("country_code") or country_code or "")

    return Location(
        zip_code=zip_code,
        latitude=lat,
        longitude=lon,
        city=city,
        department=department,
        name=", ".join(
            x for x in (city, department, selected.get("country")) if x
        ),
        country_code=country.upper() or "CO",
    )


def resolve_zip_online(
    zip_code: str,
    country_code: str | None,
    timeout: float,
    ssl_mode: str,
) -> Location:
    """
    Versión CON OpenStreet:
    1) intenta Nominatim (incluye bounding box);
    2) si OSM no dispone del ZIP, usa Open-Meteo como respaldo de geocodificación.

    Así se aprovecha la indicación del docente sin volver frágil el entregable.
    """
    try:
        return resolve_zip_openstreet(
            zip_code,
            country_code,
            timeout,
            ssl_mode,
        )
    except ChallengeError as osm_exc:
        logging.warning(
            "[OSM] ZIP %s no resuelto por OpenStreet (%s). "
            "Se intentará geocodificación de respaldo con Open-Meteo.",
            zip_code,
            osm_exc,
        )

    fallback = resolve_zip_open_meteo(
        zip_code,
        country_code,
        timeout,
        ssl_mode,
    )

    return Location(
        zip_code=fallback.zip_code,
        latitude=fallback.latitude,
        longitude=fallback.longitude,
        city=fallback.city,
        department=fallback.department,
        name=fallback.name,
        country_code=fallback.country_code,
        coordinate_source="Geocodificación Open-Meteo (respaldo)",
    )



def resolve_zip_by_source(
    zip_code: str,
    country_code: str | None,
    catalog: list[Location],
    geo_source: str,
    timeout: float,
    ssl_mode: str,
) -> Location:
    """
    Resuelve ZIP -> LAT/LON según la fuente solicitada.

    auto:
      1. catálogo local (si existe),
      2. OpenStreet/Nominatim,
      3. Open-Meteo Geocoding como respaldo.

    catalog:
      solo catálogo local/externo.

    openstreet:
      fuerza OpenStreet/Nominatim.

    openmeteo:
      fuerza Open-Meteo Geocoding.
    """
    static = find_static_zip(catalog, zip_code, country_code)

    if geo_source == "catalog":
        if static is None:
            raise LocationResolutionError(
                f"ZIP '{zip_code}' no está disponible en el catálogo."
            )
        return static

    if geo_source == "openstreet":
        return resolve_zip_openstreet(
            zip_code,
            country_code,
            timeout,
            ssl_mode,
        )

    if geo_source == "openmeteo":
        return resolve_zip_open_meteo(
            zip_code,
            country_code,
            timeout,
            ssl_mode,
        )

    # auto: prioriza datos ya conocidos para no realizar una llamada
    # geográfica innecesaria. Si no existe, intenta OSM y luego Open-Meteo.
    if static is not None:
        return static

    return resolve_zip_online(
        zip_code,
        country_code,
        timeout,
        ssl_mode,
    )

def unique_locations(
    locations: Iterable[Location],
) -> tuple[list[Location], int]:
    """
    Un ZIP representa una zona de entrada.

    Se deduplica por (country_code, zip_code), no por LAT/LON, para impedir
    llamadas redundantes cuando varias fuentes geográficas retornan centros
    ligeramente distintos para el mismo código postal.
    """
    result: list[Location] = []
    seen: set[tuple[str, str]] = set()
    removed = 0

    for loc in locations:
        key = (
            loc.country_code.upper(),
            loc.zip_code.strip().upper(),
        )

        if key in seen:
            removed += 1
            continue

        seen.add(key)
        result.append(loc)

    result.sort(
        key=lambda x: (
            normalize_text(x.department),
            normalize_text(x.city),
            x.zip_code,
        )
    )
    return result, removed



# ---------------------------------------------------------------------------
# RESOLUCIÓN DE CIUDADES: PAÍS + FUENTE POSTAL
# ---------------------------------------------------------------------------

def parse_city_spec(spec: str) -> tuple[str, str | None]:
    """
    Formatos aceptados:
      Cartagena
      Medellin
      Lima:PE
      Madrid:ES

    Si no se indica país, se infiere a partir del catálogo incorporado o
    del geocodificador. Para evitar ambigüedad, el docente puede usar :CC.
    """
    raw = spec.strip()
    if not raw:
        raise LocationResolutionError("Nombre de ciudad vacío.")

    match = re.fullmatch(r"(.+?):([A-Za-z]{2})", raw)
    if match:
        return match.group(1).strip(), match.group(2).upper()

    return raw, None


def infer_city_country(
    city_name: str,
    catalog: list[Location],
    timeout: float,
    ssl_mode: str,
) -> str:
    key = normalize_text(city_name)

    catalog_codes = {
        loc.country_code.upper()
        for loc in catalog
        if normalize_text(loc.city) == key
    }
    if len(catalog_codes) == 1:
        return next(iter(catalog_codes))

    params = {
        "name": city_name,
        "count": 10,
        "language": "es",
        "format": "json",
    }
    url = f"{GEOCODING_URL}?{urllib.parse.urlencode(params)}"

    try:
        data = http_get_json(url, timeout, ssl_mode)
    except ChallengeError as exc:
        raise LocationResolutionError(
            f"No se pudo inferir el país de '{city_name}': {exc}. "
            "Use el formato Ciudad:CC, por ejemplo Lima:PE."
        ) from exc

    results = data.get("results")
    if not isinstance(results, list) or not results:
        raise LocationResolutionError(
            f"No se pudo inferir el país de '{city_name}'. "
            "Use Ciudad:CC."
        )

    exact = [
        item for item in results
        if normalize_text(str(item.get("name", ""))) == key
        and item.get("country_code")
    ]

    selected = exact[0] if exact else results[0]
    cc = str(selected.get("country_code") or "").upper()

    if len(cc) != 2:
        raise LocationResolutionError(
            f"No se pudo determinar el país de '{city_name}'. "
            "Use Ciudad:CC."
        )

    logging.info(
        "[CIUDAD] País inferido para %s: %s",
        city_name,
        cc,
    )
    return cc


def _load_472_cache() -> dict[str, Any]:
    try:
        if CO472_CACHE_PATH.exists():
            data = json.loads(
                CO472_CACHE_PATH.read_text(encoding="utf-8")
            )
            if isinstance(data, dict):
                return data
    except Exception as exc:
        logging.warning("No se pudo leer caché 4-72: %s", exc)
    return {}


def _save_472_cache(cache: dict[str, Any]) -> None:
    try:
        CO472_CACHE_PATH.write_text(
            json.dumps(cache, ensure_ascii=False),
            encoding="utf-8",
        )
    except Exception as exc:
        logging.warning("No se pudo guardar caché 4-72: %s", exc)


def _arcgis_features(
    url: str,
    params: dict[str, Any],
    timeout: float,
    ssl_mode: str,
) -> list[dict[str, Any]]:
    """
    Consulta una capa ArcGIS y pagina cuando el servidor indica que hay más
    resultados. Esto evita perder entidades silenciosamente en consultas
    amplias (por ejemplo, departamentos o municipios grandes).
    """
    page_size = 2000
    offset = 0
    result: list[dict[str, Any]] = []
    seen_fingerprints: set[str] = set()

    while True:
        request_params = {
            **params,
            "f": "json",
            "resultOffset": offset,
            "resultRecordCount": page_size,
        }

        data = http_post_form_json(
            url,
            request_params,
            timeout,
            ssl_mode,
            user_agent="UTB-Parallel-Computing-Challenge7/2.0",
        )

        features = data.get("features")
        if not isinstance(features, list):
            break

        page = [f for f in features if isinstance(f, dict)]
        if not page:
            break

        # Defensa contra servidores que ignoren resultOffset.
        first = page[0].get("attributes") if page else None
        last = page[-1].get("attributes") if page else None
        fingerprint = json.dumps(
            [first, last, len(page)],
            sort_keys=True,
            ensure_ascii=False,
            default=str,
        )
        if fingerprint in seen_fingerprints:
            logging.warning(
                "[4-72] El servicio repitió una página; se detiene la paginación "
                "para evitar un ciclo infinito."
            )
            break
        seen_fingerprints.add(fingerprint)

        result.extend(page)

        exceeded = bool(data.get("exceededTransferLimit"))
        if not exceeded and len(page) < page_size:
            break

        offset += len(page)

    return result


def _department_name_map_472(
    timeout: float,
    ssl_mode: str,
) -> dict[str, str]:
    cache = _load_472_cache()
    cached = cache.get("departments")
    if isinstance(cached, dict):
        return {
            str(k): str(v)
            for k, v in cached.items()
        }

    features = _arcgis_features(
        CO472_DEPARTAMENTO_QUERY_URL,
        {
            "where": "1=1",
            "outFields": (
                "OBJECTID,Codigo,Nombre,Identificador,Codigo_DANE"
            ),
            "returnGeometry": "false",
        },
        timeout,
        ssl_mode,
    )

    mapping: dict[str, str] = {}

    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict):
            continue

        name = str(attrs.get("Nombre") or "").strip()
        if not name:
            continue

        for field in (
            "OBJECTID",
            "Codigo",
            "Identificador",
            "Codigo_DANE",
        ):
            value = attrs.get(field)
            if value is not None and str(value).strip():
                mapping[str(value).strip()] = name

    cache["departments"] = mapping
    _save_472_cache(cache)
    return mapping



def _department_record_472(
    department_name: str,
    timeout: float,
    ssl_mode: str,
) -> tuple[int, str, str]:
    """
    Devuelve:
      OBJECTID del departamento,
      nombre oficial,
      código DANE.

    La capa oficial de departamentos de 4-72 es poligonal y publica
    OBJECTID, Nombre y Codigo_DANE.
    """
    cache = _load_472_cache()
    key = f"departamento:{normalize_text(department_name)}"
    cached = cache.get(key)

    if isinstance(cached, dict):
        try:
            return (
                int(cached["objectid"]),
                str(cached["name"]),
                str(cached["dane"]),
            )
        except (KeyError, TypeError, ValueError):
            pass

    features = _arcgis_features(
        CO472_DEPARTAMENTO_QUERY_URL,
        {
            "where": "1=1",
            "outFields": (
                "OBJECTID,Codigo,Nombre,Identificador,Codigo_DANE"
            ),
            "returnGeometry": "false",
        },
        timeout,
        ssl_mode,
    )

    wanted = normalize_text(department_name)

    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict):
            continue

        name = str(attrs.get("Nombre") or "").strip()

        if normalize_text(name) != wanted:
            continue

        try:
            objectid = int(attrs["OBJECTID"])
        except (KeyError, TypeError, ValueError):
            continue

        dane = str(attrs.get("Codigo_DANE") or "").strip()

        if not dane:
            dane = str(attrs.get("Codigo") or "").strip()

        cache[key] = {
            "objectid": objectid,
            "name": name,
            "dane": dane,
        }
        _save_472_cache(cache)

        return objectid, name, dane

    raise LocationResolutionError(
        f"4-72 no encontró el departamento '{department_name}'."
    )


def _department_geometry_472(
    objectid: int,
    timeout: float,
    ssl_mode: str,
) -> dict[str, Any]:
    features = _arcgis_features(
        CO472_DEPARTAMENTO_QUERY_URL,
        {
            "where": f"OBJECTID={int(objectid)}",
            "outFields": "OBJECTID",
            "returnGeometry": "true",
            "outSR": "4326",
        },
        timeout,
        ssl_mode,
    )

    if not features:
        raise LocationResolutionError(
            "4-72 no devolvió la geometría del departamento."
        )

    geometry = features[0].get("geometry")

    if not isinstance(geometry, dict):
        raise LocationResolutionError(
            "4-72 devolvió una geometría departamental inválida."
        )

    return geometry


def discover_colombia_department_postcodes_472(
    department_name: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Departamento colombiano -> todos los códigos postales candidatos de 4-72.

    Se combina:
      1) polígono oficial del departamento;
      2) intersección espacial con la capa CodigoPostal;
      3) prefijo DANE del código postal para descartar zonas vecinas que
         únicamente toquen el límite departamental.

    No requiere un CSV externo.
    """
    cache = _load_472_cache()
    cache_key = f"postal_departamento:{normalize_text(department_name)}"
    cached = cache.get(cache_key)

    if isinstance(cached, list) and cached:
        result: list[Location] = []

        for item in cached:
            if not isinstance(item, dict):
                continue
            try:
                result.append(
                    Location(
                        zip_code=str(item["zip_code"]),
                        latitude=float(item["latitude"]),
                        longitude=float(item["longitude"]),
                        city=str(item.get("city") or ""),
                        department=str(item["department"]),
                        name=str(item["name"]),
                        country_code="CO",
                        coordinate_source="4-72 / MinTIC",
                        minlon=item.get("minlon"),
                        minlat=item.get("minlat"),
                        maxlon=item.get("maxlon"),
                        maxlat=item.get("maxlat"),
                    )
                )
            except (KeyError, TypeError, ValueError):
                pass

        if result:
            logging.info(
                "[4-72] Caché departamento: %s -> %d códigos postales",
                department_name,
                len(result),
            )
            return result

    objectid, official_name, dane_code = _department_record_472(
        department_name,
        timeout,
        ssl_mode,
    )
    department_geometry = _department_geometry_472(
        objectid,
        timeout,
        ssl_mode,
    )

    features = _arcgis_features(
        CO472_POSTAL_QUERY_URL,
        {
            "where": "1=1",
            "geometry": json.dumps(
                department_geometry,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "geometryType": "esriGeometryPolygon",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "Codigo_Postal,Nombre",
            "returnGeometry": "true",
            "outSR": "4326",
        },
        max(timeout, 60.0),
        ssl_mode,
    )

    # En Colombia los primeros dos dígitos corresponden al código
    # departamental. Se usa como segunda barrera contra intersecciones
    # fronterizas espurias.
    digits = "".join(ch for ch in dane_code if ch.isdigit())
    dane_prefix = digits[:2] if len(digits) >= 2 else ""

    by_zip: dict[str, Location] = {}
    rejected: list[str] = []

    for feature in features:
        attrs = feature.get("attributes")
        geometry = feature.get("geometry")

        if not isinstance(attrs, dict) or not isinstance(geometry, dict):
            continue

        zip_code = str(attrs.get("Codigo_Postal") or "").strip()

        if not re.fullmatch(r"\d{6}", zip_code):
            continue

        if dane_prefix and not zip_code.startswith(dane_prefix):
            rejected.append(zip_code)
            continue

        point = _polygon_representative_point(geometry)
        if point is None:
            continue

        lat, lon = point

        # Si no se obtuvo un código DANE utilizable, se exige que el punto
        # representativo esté dentro del polígono departamental.
        if (
            not dane_prefix
            and not _point_in_polygon_geometry(
                lon,
                lat,
                department_geometry,
            )
        ):
            rejected.append(zip_code)
            continue

        minlon, minlat, maxlon, maxlat = _geometry_bbox(geometry)

        by_zip[zip_code] = Location(
            zip_code=zip_code,
            latitude=lat,
            longitude=lon,
            city=f"Departamento {official_name}",
            department=official_name,
            name=f"{official_name} - ZIP {zip_code}",
            country_code="CO",
            coordinate_source="4-72 / MinTIC",
            minlon=minlon,
            minlat=minlat,
            maxlon=maxlon,
            maxlat=maxlat,
        )

    result = sorted(
        by_zip.values(),
        key=lambda loc: loc.zip_code,
    )

    if not result:
        raise LocationResolutionError(
            f"4-72 no devolvió códigos postales válidos para "
            f"'{official_name}'."
        )

    if rejected:
        logging.info(
            "[4-72] %s: se descartaron %d candidato(s) fronterizos/no "
            "pertenecientes.",
            official_name,
            len(set(rejected)),
        )

    cache[cache_key] = [
        {
            "zip_code": loc.zip_code,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "city": loc.city,
            "department": loc.department,
            "name": loc.name,
            "minlon": loc.minlon,
            "minlat": loc.minlat,
            "maxlon": loc.maxlon,
            "maxlat": loc.maxlat,
        }
        for loc in result
    ]
    _save_472_cache(cache)

    logging.info(
        "[4-72] Departamento %s -> %d códigos postales validados",
        official_name,
        len(result),
    )

    return result


def expand_department_final(
    department_spec: str,
    catalog: list[Location],
    geo_source: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Departamento -> conjunto de códigos postales.

    Para Colombia, auto/colombia472 usa 4-72/MinTIC.
    Para otros países o cuando se fuerza catalog, se requiere cobertura
    explícita en el CSV.
    """
    department_name, explicit_cc = parse_city_spec(department_spec)
    country_code = (explicit_cc or "CO").upper()

    if country_code == "CO" and geo_source in ("auto", "colombia472"):
        try:
            return discover_colombia_department_postcodes_472(
                department_name,
                timeout,
                ssl_mode,
            )
        except ChallengeError:
            if geo_source == "colombia472":
                raise
            logging.warning(
                "[4-72] No se pudo resolver automáticamente el "
                "departamento %s; se intentará catálogo.",
                department_name,
            )

    key = normalize_text(department_name)
    matches = [
        loc
        for loc in catalog
        if normalize_text(loc.department) == key
        and loc.country_code.upper() == country_code
    ]
    unique, _ = unique_locations(matches)

    if not unique:
        raise LocationResolutionError(
            f"No fue posible resolver el departamento "
            f"'{department_name}:{country_code}'. "
            "Para un país distinto de Colombia use --catalog con cobertura "
            "postal completa."
        )

    return unique




def _infer_city_admin1_openmeteo(
    city_name: str,
    timeout: float,
    ssl_mode: str,
) -> str:
    """Obtiene el departamento/región de una ciudad para desambiguar homónimos."""
    params = {
        "name": city_name,
        "count": 10,
        "language": "es",
        "format": "json",
        "countryCode": "CO",
    }
    url = f"{GEOCODING_URL}?{urllib.parse.urlencode(params)}"

    try:
        data = http_get_json(url, timeout, ssl_mode)
    except ChallengeError:
        return ""

    results = data.get("results")
    if not isinstance(results, list):
        return ""

    wanted = normalize_text(city_name)
    for item in results:
        if not isinstance(item, dict):
            continue
        if str(item.get("country_code") or "").upper() != "CO":
            continue
        if normalize_text(str(item.get("name") or "")) != wanted:
            continue
        admin1 = str(item.get("admin1") or "").strip()
        if admin1:
            return admin1
    return ""


def _municipality_dane_prefix(dane_code: str) -> str:
    digits = "".join(ch for ch in str(dane_code) if ch.isdigit())
    return digits[:5] if len(digits) >= 5 else ""


def _official_postcodes_by_municipality_dane(
    dane_code: str,
    timeout: float,
    ssl_mode: str,
) -> set[str]:
    """
    Obtiene códigos postales vinculados a manzanas DANE del municipio usando
    la capa oficial CodigoPostalAmpliado de 4-72/MinTIC.

    Es una validación de alta confianza. Si la capa ampliada no cubre una zona
    rural concreta, el algoritmo conserva además la validación espacial como
    respaldo; nunca se inventan códigos postales.
    """
    prefix = _municipality_dane_prefix(dane_code)
    if not prefix:
        return set()

    cache = _load_472_cache()
    cache_key = f"dane_postcodes:{prefix}"
    cached = cache.get(cache_key)
    if isinstance(cached, list):
        values = {str(x) for x in cached if re.fullmatch(r"\d{6}", str(x))}
        if values:
            return values

    try:
        features = _arcgis_features(
            CO472_POSTAL_EXPANDED_QUERY_URL,
            {
                "where": f"CodigoManzana LIKE '{prefix}%'",
                "outFields": "CodigoPostal",
                "returnGeometry": "false",
                "returnDistinctValues": "true",
                "orderByFields": "CodigoPostal",
            },
            max(timeout, 45.0),
            ssl_mode,
        )
    except ChallengeError as exc:
        logging.warning(
            "[4-72] No se pudo consultar asociación DANE->código postal para %s: %s",
            prefix,
            exc,
        )
        return set()

    result: set[str] = set()
    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict):
            continue
        code = str(attrs.get("CodigoPostal") or "").strip()
        if re.fullmatch(r"\d{6}", code):
            result.add(code)

    if result:
        cache[cache_key] = sorted(result)
        _save_472_cache(cache)
        logging.info(
            "[4-72] DANE %s -> %d código(s) postal(es) asociados oficialmente",
            prefix,
            len(result),
        )

    return result


def _municipality_record_472(
    city_name: str,
    expected_department: str,
    timeout: float,
    ssl_mode: str,
) -> tuple[int, str, str]:
    """
    Busca el municipio en la división administrativa oficial 4-72.

    Devuelve OBJECTID, departamento y código DANE. Si existen municipios
    homónimos, intenta desambiguarlos por departamento y, si aún no es posible,
    falla explícitamente en lugar de escoger uno al azar.
    """
    cache = _load_472_cache()
    cache_key = (
        f"municipio:{normalize_text(city_name)}:"
        f"{normalize_text(expected_department)}"
    )
    cached = cache.get(cache_key)

    if isinstance(cached, dict):
        try:
            return (
                int(cached["objectid"]),
                str(cached["department"]),
                str(cached.get("dane_code") or ""),
            )
        except (KeyError, TypeError, ValueError):
            pass

    features = _arcgis_features(
        CO472_MUNICIPIO_QUERY_URL,
        {
            "where": "1=1",
            "outFields": (
                "OBJECTID,Municipio,Nombre,DepartamentoID,Codigo_DANE"
            ),
            "returnGeometry": "false",
        },
        timeout,
        ssl_mode,
    )

    wanted = normalize_text(city_name)
    dep_map = _department_name_map_472(timeout, ssl_mode)
    matches: list[tuple[int, str, str]] = []

    for feature in features:
        attrs = feature.get("attributes")
        if not isinstance(attrs, dict):
            continue

        names = (attrs.get("Municipio"), attrs.get("Nombre"))
        if not any(
            normalize_text(str(value)) == wanted
            for value in names
            if value
        ):
            continue

        try:
            objectid = int(attrs["OBJECTID"])
        except (KeyError, TypeError, ValueError):
            continue

        dep_id = str(attrs.get("DepartamentoID") or "").strip()
        dep_name = dep_map.get(dep_id, "")
        dane_code = str(attrs.get("Codigo_DANE") or "").strip()
        matches.append((objectid, dep_name, dane_code))

    resolved_department = expected_department.strip()
    if not resolved_department and len(matches) > 1:
        inferred = _infer_city_admin1_openmeteo(city_name, timeout, ssl_mode)
        if inferred:
            resolved_department = inferred
            logging.info(
                "[4-72] Municipio homónimo '%s': departamento inferido %s",
                city_name,
                inferred,
            )

    if resolved_department:
        exact = [
            item for item in matches
            if normalize_text(item[1]) == normalize_text(resolved_department)
        ]
        if exact:
            matches = exact

    if not matches:
        raise LocationResolutionError(
            f"4-72 no encontró el municipio '{city_name}'"
            + (f" en {resolved_department}" if resolved_department else "")
            + "."
        )

    if len(matches) > 1:
        departments = sorted({item[1] for item in matches if item[1]})
        raise LocationResolutionError(
            f"El municipio '{city_name}' es ambiguo en 4-72. "
            f"Coincidencias: {', '.join(departments) or 'varias'}. "
            "Use una ciudad inequívoca o un catálogo explícito."
        )

    objectid, dep_name, dane_code = matches[0]
    final_dep = dep_name or resolved_department

    cache[cache_key] = {
        "objectid": objectid,
        "department": final_dep,
        "dane_code": dane_code,
    }
    _save_472_cache(cache)

    return objectid, final_dep, dane_code


def _municipality_geometry_472(
    objectid: int,
    timeout: float,
    ssl_mode: str,
) -> dict[str, Any]:
    features = _arcgis_features(
        CO472_MUNICIPIO_QUERY_URL,
        {
            "where": f"OBJECTID={int(objectid)}",
            "outFields": "OBJECTID",
            "returnGeometry": "true",
            "outSR": "4326",
        },
        timeout,
        ssl_mode,
    )

    if not features:
        raise LocationResolutionError(
            "4-72 no devolvió la geometría del municipio."
        )

    geometry = features[0].get("geometry")
    if not isinstance(geometry, dict):
        raise LocationResolutionError(
            "4-72 devolvió una geometría municipal inválida."
        )

    return geometry


def _ring_centroid(
    ring: list[Any],
) -> tuple[float, float] | None:
    points: list[tuple[float, float]] = []

    for value in ring:
        if (
            isinstance(value, list)
            and len(value) >= 2
        ):
            try:
                points.append(
                    (float(value[0]), float(value[1]))
                )
            except (TypeError, ValueError):
                continue

    if len(points) < 3:
        return None

    if points[0] != points[-1]:
        points.append(points[0])

    area2 = 0.0
    cx_num = 0.0
    cy_num = 0.0

    for (x1, y1), (x2, y2) in zip(
        points,
        points[1:],
    ):
        cross = x1 * y2 - x2 * y1
        area2 += cross
        cx_num += (x1 + x2) * cross
        cy_num += (y1 + y2) * cross

    if abs(area2) < 1e-12:
        xs = [p[0] for p in points[:-1]]
        ys = [p[1] for p in points[:-1]]
        return (
            sum(xs) / len(xs),
            sum(ys) / len(ys),
        )

    return (
        cx_num / (3.0 * area2),
        cy_num / (3.0 * area2),
    )


def _polygon_representative_point(
    geometry: dict[str, Any],
) -> tuple[float, float] | None:
    rings = geometry.get("rings")
    if not isinstance(rings, list) or not rings:
        return None

    candidates: list[
        tuple[float, tuple[float, float]]
    ] = []

    for ring in rings:
        if not isinstance(ring, list):
            continue

        centroid = _ring_centroid(ring)
        if centroid is None:
            continue

        # Área aproximada del anillo para seleccionar el anillo principal.
        area = 0.0
        pts = []
        for value in ring:
            if isinstance(value, list) and len(value) >= 2:
                try:
                    pts.append(
                        (float(value[0]), float(value[1]))
                    )
                except (TypeError, ValueError):
                    pass

        if len(pts) >= 3:
            if pts[0] != pts[-1]:
                pts.append(pts[0])
            area = abs(
                sum(
                    x1 * y2 - x2 * y1
                    for (x1, y1), (x2, y2)
                    in zip(pts, pts[1:])
                )
            )

        candidates.append((area, centroid))

    if not candidates:
        return None

    _, (lon, lat) = max(
        candidates,
        key=lambda item: item[0],
    )
    return lat, lon



def _point_in_ring(
    lon: float,
    lat: float,
    ring: list[Any],
) -> bool:
    """
    Prueba ray-casting 2D. Los puntos sobre borde se consideran dentro
    mediante una tolerancia pequeña.
    """
    points: list[tuple[float, float]] = []

    for value in ring:
        if isinstance(value, list) and len(value) >= 2:
            try:
                points.append((float(value[0]), float(value[1])))
            except (TypeError, ValueError):
                pass

    if len(points) < 3:
        return False

    if points[0] != points[-1]:
        points.append(points[0])

    eps = 1e-10

    # Borde primero.
    for (x1, y1), (x2, y2) in zip(points, points[1:]):
        dx = x2 - x1
        dy = y2 - y1
        cross = (lon - x1) * dy - (lat - y1) * dx

        if abs(cross) <= eps:
            if (
                min(x1, x2) - eps <= lon <= max(x1, x2) + eps
                and min(y1, y2) - eps <= lat <= max(y1, y2) + eps
            ):
                return True

    inside = False
    j = len(points) - 1

    for i in range(len(points)):
        xi, yi = points[i]
        xj, yj = points[j]

        intersects = (
            (yi > lat) != (yj > lat)
            and lon
            < (xj - xi) * (lat - yi) / ((yj - yi) or eps) + xi
        )

        if intersects:
            inside = not inside

        j = i

    return inside


def _point_in_polygon_geometry(
    lon: float,
    lat: float,
    geometry: dict[str, Any],
) -> bool:
    """
    ArcGIS puede devolver polígonos multipartes y anillos internos.
    La regla par/impar sobre todos los anillos funciona para outer rings
    y holes sin depender de su orientación.
    """
    rings = geometry.get("rings")
    if not isinstance(rings, list):
        return False

    crossings = 0

    for ring in rings:
        if isinstance(ring, list) and _point_in_ring(lon, lat, ring):
            crossings += 1

    return crossings % 2 == 1


def _geometry_sample_points(
    geometry: dict[str, Any],
    grid_size: int = 9,
) -> list[tuple[float, float]]:
    """Genera puntos interiores de una geometría para estimar solapamiento."""
    minlon, minlat, maxlon, maxlat = _geometry_bbox(geometry)
    if None in (minlon, minlat, maxlon, maxlat):
        return []

    assert minlon is not None and minlat is not None
    assert maxlon is not None and maxlat is not None

    if grid_size < 3:
        grid_size = 3

    result: list[tuple[float, float]] = []
    for iy in range(grid_size):
        lat = minlat + (maxlat - minlat) * (iy + 0.5) / grid_size
        for ix in range(grid_size):
            lon = minlon + (maxlon - minlon) * (ix + 0.5) / grid_size
            if _point_in_polygon_geometry(lon, lat, geometry):
                result.append((lat, lon))
    return result


def _postal_overlap_with_municipality(
    postal_geometry: dict[str, Any],
    municipality_geometry: dict[str, Any],
) -> tuple[float, tuple[float, float] | None]:
    """
    Estima qué fracción de la zona postal está dentro del municipio mediante
    muestreo espacial. Devuelve también un punto que pertenece a ambas zonas.
    """
    samples = _geometry_sample_points(postal_geometry, grid_size=9)
    if not samples:
        point = _polygon_representative_point(postal_geometry)
        if point is None:
            return 0.0, None
        lat, lon = point
        inside = _point_in_polygon_geometry(lon, lat, municipality_geometry)
        return (1.0 if inside else 0.0), (point if inside else None)

    inside_both = [
        point for point in samples
        if _point_in_polygon_geometry(point[1], point[0], municipality_geometry)
    ]
    ratio = len(inside_both) / len(samples)

    if inside_both:
        # Elegir un punto central entre las muestras que sí pertenecen a ambas zonas.
        lat = sum(p[0] for p in inside_both) / len(inside_both)
        lon = sum(p[1] for p in inside_both) / len(inside_both)
        if (
            _point_in_polygon_geometry(lon, lat, postal_geometry)
            and _point_in_polygon_geometry(lon, lat, municipality_geometry)
        ):
            return ratio, (lat, lon)
        return ratio, inside_both[len(inside_both) // 2]

    return ratio, None


def _postal_point_is_inside_municipality(
    postal_geometry: dict[str, Any],
    municipality_geometry: dict[str, Any],
) -> tuple[bool, tuple[float, float] | None]:
    ratio, overlap_point = _postal_overlap_with_municipality(
        postal_geometry,
        municipality_geometry,
    )

    # Mayoría espacial: evita incluir polígonos vecinos que solo tocan el borde.
    if ratio >= 0.50:
        return True, overlap_point or _polygon_representative_point(postal_geometry)

    return False, overlap_point


def _geometry_bbox(
    geometry: dict[str, Any],
) -> tuple[
    float | None,
    float | None,
    float | None,
    float | None,
]:
    rings = geometry.get("rings")
    if not isinstance(rings, list):
        return None, None, None, None

    xs: list[float] = []
    ys: list[float] = []

    for ring in rings:
        if not isinstance(ring, list):
            continue
        for value in ring:
            if isinstance(value, list) and len(value) >= 2:
                try:
                    xs.append(float(value[0]))
                    ys.append(float(value[1]))
                except (TypeError, ValueError):
                    pass

    if not xs or not ys:
        return None, None, None, None

    return min(xs), min(ys), max(xs), max(ys)


def discover_colombia_city_postcodes_472(
    city_name: str,
    expected_department: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Ciudad colombiana -> polígonos de códigos postales oficiales 4-72.

    La geometría municipal se usa directamente como filtro espacial de la
    capa CodigoPostal; no se obliga a Nominatim a conocer cada ZIP.
    """
    cache = _load_472_cache()
    cache_key = (
        f"postal:{normalize_text(city_name)}:"
        f"{normalize_text(expected_department)}"
    )

    cached = cache.get(cache_key)
    if isinstance(cached, list) and cached:
        result: list[Location] = []
        for item in cached:
            if not isinstance(item, dict):
                continue
            try:
                result.append(
                    Location(
                        zip_code=str(item["zip_code"]),
                        latitude=float(item["latitude"]),
                        longitude=float(item["longitude"]),
                        city=city_name,
                        department=str(item["department"]),
                        name=str(item["name"]),
                        country_code="CO",
                        coordinate_source="4-72 / MinTIC",
                        minlon=item.get("minlon"),
                        minlat=item.get("minlat"),
                        maxlon=item.get("maxlon"),
                        maxlat=item.get("maxlat"),
                    )
                )
            except (KeyError, TypeError, ValueError):
                pass

        if result:
            logging.info(
                "[4-72] Caché: %s -> %d códigos postales",
                city_name,
                len(result),
            )
            return result

    objectid, department, dane_code = _municipality_record_472(
        city_name,
        expected_department,
        timeout,
        ssl_mode,
    )
    municipality_geometry = _municipality_geometry_472(
        objectid,
        timeout,
        ssl_mode,
    )

    features = _arcgis_features(
        CO472_POSTAL_QUERY_URL,
        {
            "where": "1=1",
            "geometry": json.dumps(
                municipality_geometry,
                ensure_ascii=False,
                separators=(",", ":"),
            ),
            "geometryType": "esriGeometryPolygon",
            "inSR": "4326",
            "spatialRel": "esriSpatialRelIntersects",
            "outFields": "Codigo_Postal,Nombre",
            "returnGeometry": "true",
            "outSR": "4326",
        },
        max(timeout, 45.0),
        ssl_mode,
    )

    by_zip: dict[str, Location] = {}
    rejected_outside: list[str] = []

    # Validación municipal en capas, de mayor a menor confianza:
    # 1) asociación oficial CodigoManzana(DANE) -> CodigoPostal;
    # 2) lista verificada local para casos de curso;
    # 3) solapamiento espacial mayoritario con el polígono municipal.
    dane_verified_codes = _official_postcodes_by_municipality_dane(
        dane_code,
        timeout,
        ssl_mode,
    )
    known_verified_codes = set(_known_city_postcodes(city_name))
    verified_city_codes = dane_verified_codes | known_verified_codes

    if dane_verified_codes:
        logging.info(
            "[4-72] %s: validación postal primaria por código DANE %s (%d códigos)",
            city_name,
            _municipality_dane_prefix(dane_code),
            len(dane_verified_codes),
        )

    for feature in features:
        attrs = feature.get("attributes")
        geometry = feature.get("geometry")
        if not isinstance(attrs, dict) or not isinstance(
            geometry,
            dict,
        ):
            continue

        zip_code = str(
            attrs.get("Codigo_Postal")
            or ""
        ).strip()

        if not re.fullmatch(r"\d{6}", zip_code):
            continue

        point = _polygon_representative_point(geometry)
        if point is None:
            continue

        membership_verified = zip_code in verified_city_codes
        overlap_point: tuple[float, float] | None = None

        if not membership_verified:
            inside, overlap_point = _postal_point_is_inside_municipality(
                geometry,
                municipality_geometry,
            )
            if not inside:
                rejected_outside.append(zip_code)
                continue

        # Para candidatos espaciales se prefiere un punto que esté simultáneamente
        # dentro de la zona postal y del municipio. Para membresía DANE oficial se
        # conserva el punto interior representativo de la zona postal.
        lat, lon = overlap_point or point
        minlon, minlat, maxlon, maxlat = _geometry_bbox(
            geometry
        )

        by_zip[zip_code] = Location(
            zip_code=zip_code,
            latitude=lat,
            longitude=lon,
            city=city_name,
            department=department or expected_department,
            name=f"{city_name} - ZIP {zip_code}",
            country_code="CO",
            coordinate_source="4-72 / MinTIC",
            minlon=minlon,
            minlat=minlat,
            maxlon=maxlon,
            maxlat=maxlat,
        )

    # Si existe un conjunto municipal verificado, debe quedar completo. Si un ZIP
    # conocido no llegó en la consulta espacial general, se solicita por
    # código directamente a 4-72 y se conserva la geometría oficial.
    if verified_city_codes:
        missing_verified = sorted(
            verified_city_codes - set(by_zip)
        )

        if missing_verified:
            where = "Codigo_Postal IN (" + ",".join(
                f"'{code}'" for code in missing_verified
            ) + ")"

            extra_features = _arcgis_features(
                CO472_POSTAL_QUERY_URL,
                {
                    "where": where,
                    "outFields": "Codigo_Postal,Nombre",
                    "returnGeometry": "true",
                    "outSR": "4326",
                },
                max(timeout, 45.0),
                ssl_mode,
            )

            for feature in extra_features:
                attrs = feature.get("attributes")
                geometry = feature.get("geometry")
                if not isinstance(attrs, dict) or not isinstance(
                    geometry,
                    dict,
                ):
                    continue

                zip_code = str(
                    attrs.get("Codigo_Postal")
                    or ""
                ).strip()

                if zip_code not in verified_city_codes:
                    continue

                point = _polygon_representative_point(geometry)
                if point is None:
                    continue

                lat, lon = point
                minlon, minlat, maxlon, maxlat = _geometry_bbox(
                    geometry
                )

                by_zip[zip_code] = Location(
                    zip_code=zip_code,
                    latitude=lat,
                    longitude=lon,
                    city=city_name,
                    department=department or expected_department,
                    name=f"{city_name} - ZIP {zip_code}",
                    country_code="CO",
                    coordinate_source="4-72 / MinTIC",
                    minlon=minlon,
                    minlat=minlat,
                    maxlon=maxlon,
                    maxlat=maxlat,
                )

        still_missing = sorted(
            verified_city_codes - set(by_zip)
        )
        if still_missing:
            raise LocationResolutionError(
                f"4-72 no devolvió geometría para los códigos postales "
                f"municipales verificados de {city_name}: "
                f"{', '.join(still_missing)}."
            )

    if rejected_outside:
        logging.info(
            "[4-72] %s: se descartaron %d candidato(s) de municipios "
            "vecinos/no pertenecientes: %s",
            city_name,
            len(set(rejected_outside)),
            ", ".join(sorted(set(rejected_outside))),
        )

    result = sorted(
        by_zip.values(),
        key=lambda loc: loc.zip_code,
    )

    if not result:
        raise LocationResolutionError(
            f"4-72 no devolvió códigos postales para "
            f"'{city_name}'."
        )

    cache[cache_key] = [
        {
            "zip_code": loc.zip_code,
            "latitude": loc.latitude,
            "longitude": loc.longitude,
            "department": loc.department,
            "name": loc.name,
            "minlon": loc.minlon,
            "minlat": loc.minlat,
            "maxlon": loc.maxlon,
            "maxlat": loc.maxlat,
        }
        for loc in result
    ]
    _save_472_cache(cache)

    logging.info(
        "[4-72] %s, %s -> %d códigos postales municipales validados: %s",
        city_name,
        department or expected_department,
        len(result),
        ", ".join(loc.zip_code for loc in result),
    )

    return result


def _generic_postcode_values(
    raw_value: Any,
    country_code: str,
) -> list[str]:
    if raw_value is None:
        return []

    chunks = re.split(
        r"[;,|]+",
        str(raw_value),
    )
    result: list[str] = []

    for chunk in chunks:
        value = " ".join(chunk.strip().split())
        if not value:
            continue

        if country_code.upper() == "CO":
            if re.fullmatch(r"\d{6}", value):
                result.append(value)
        elif re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9 -]{1,10}[A-Za-z0-9]",
            value,
        ):
            result.append(value.upper())

    return result



def _nominatim_bbox(
    result: dict[str, Any],
) -> tuple[float, float, float, float] | None:
    """Nominatim: [sur, norte, oeste, este] -> (sur, oeste, norte, este)."""
    bbox = result.get("boundingbox")
    if not isinstance(bbox, list) or len(bbox) != 4:
        return None
    try:
        south = float(bbox[0])
        north = float(bbox[1])
        west = float(bbox[2])
        east = float(bbox[3])
    except (TypeError, ValueError):
        return None
    if south >= north or west >= east:
        return None
    return south, west, north, east


def _overpass_request_with_fallback(
    query: str,
    timeout: float,
    ssl_mode: str,
) -> dict[str, Any]:
    """Prueba dos instancias públicas de Overpass antes de abandonar."""
    last_error: ChallengeError | None = None
    for endpoint in OVERPASS_URLS:
        url = endpoint + "?" + urllib.parse.urlencode({"data": query})
        try:
            return http_get_json(url, max(timeout, 40.0), ssl_mode)
        except ChallengeError as exc:
            last_error = exc
            logging.warning("[OSM] Overpass falló en %s: %s", endpoint, exc)
    if last_error is not None:
        raise last_error
    raise APIRequestError("No hay endpoint Overpass disponible.")


def _overpass_postcode_query_for_area(area_id: int) -> str:
    return f"""
[out:json][timeout:35];
area({area_id})->.searchArea;
(
  rel(area.searchArea)["boundary"="postal_code"]["postal_code"];
  way(area.searchArea)["boundary"="postal_code"]["postal_code"];
  rel(area.searchArea)["postal_code"];
  way(area.searchArea)["postal_code"];
  node(area.searchArea)["postal_code"];
  node(area.searchArea)["addr:postcode"];
  way(area.searchArea)["addr:postcode"];
  rel(area.searchArea)["addr:postcode"];
);
out center tags;
""".strip()


def _overpass_postcode_query_for_bbox(
    bbox: tuple[float, float, float, float],
) -> str:
    south, west, north, east = bbox
    box = f"{south:.7f},{west:.7f},{north:.7f},{east:.7f}"
    return f"""
[out:json][timeout:35];
(
  rel["boundary"="postal_code"]["postal_code"]({box});
  way["boundary"="postal_code"]["postal_code"]({box});
  rel["postal_code"]({box});
  way["postal_code"]({box});
  node["postal_code"]({box});
  node["addr:postcode"]({box});
  way["addr:postcode"]({box});
  rel["addr:postcode"]({box});
);
out center tags;
""".strip()


def _center_inside_bbox(
    center: tuple[float, float],
    bbox: tuple[float, float, float, float] | None,
) -> bool:
    if bbox is None:
        return True
    lat, lon = center
    south, west, north, east = bbox
    return south <= lat <= north and west <= lon <= east


def _locations_from_overpass_postcodes(
    data: dict[str, Any],
    city_name: str,
    country_code: str,
    department: str,
    bbox: tuple[float, float, float, float] | None,
    source_label: str,
) -> list[Location]:
    elements = data.get("elements")
    if not isinstance(elements, list):
        return []

    # postcode -> (prioridad, Location). Las fronteras postales son preferidas
    # sobre códigos observados solo en direcciones individuales.
    by_postcode: dict[str, tuple[int, Location]] = {}

    for element in elements:
        if not isinstance(element, dict):
            continue
        tags = element.get("tags")
        if not isinstance(tags, dict):
            tags = {}

        candidates: list[tuple[int, str]] = []
        for code in _generic_postcode_values(tags.get("postal_code"), country_code):
            priority = 0 if tags.get("boundary") == "postal_code" else 1
            candidates.append((priority, code))
        for code in _generic_postcode_values(tags.get("addr:postcode"), country_code):
            candidates.append((2, code))

        center = _element_center(element)
        if center is None or not _center_inside_bbox(center, bbox):
            continue
        lat, lon = center

        for priority, postcode in candidates:
            current = by_postcode.get(postcode)
            if current is not None and current[0] <= priority:
                continue
            by_postcode[postcode] = (
                priority,
                Location(
                    zip_code=postcode,
                    latitude=lat,
                    longitude=lon,
                    city=city_name,
                    department=department,
                    name=f"{city_name} - ZIP {postcode}",
                    country_code=country_code.upper(),
                    coordinate_source=source_label,
                ),
            )

    return sorted((value[1] for value in by_postcode.values()), key=lambda x: x.zip_code)


def discover_city_postcodes_openstreet_generic(
    city_name: str,
    country_code: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Resolución internacional de mejor esfuerzo.

    1) Nominatim identifica la ciudad.
    2) Si existe relación/vía administrativa, Overpass consulta por área.
    3) Si no existe área, o el área no devuelve postcodes, se usa la caja
       delimitadora de Nominatim como respaldo.
    4) Se prefieren límites postales sobre postcodes de direcciones aisladas.

    La cobertura internacional depende de OSM; si no hay datos suficientes se
    falla explícitamente y nunca se inventa una zona postal.
    """
    cc = country_code.lower()
    requested = normalize_text(city_name)

    try:
        results = nominatim_search(
            {
                "city": city_name,
                "countrycodes": cc,
                "limit": 10,
            },
            timeout,
            ssl_mode,
        )
    except ChallengeError as exc:
        logging.warning(
            "[OSM] No se pudo buscar '%s:%s': %s",
            city_name,
            country_code,
            exc,
        )
        return []

    if not results:
        return []

    selected = results[0]
    for candidate in results:
        address = candidate.get("address")
        if not isinstance(address, dict):
            address = {}
        values = (
            candidate.get("name"),
            address.get("city"),
            address.get("town"),
            address.get("municipality"),
            address.get("village"),
        )
        if any(
            normalize_text(str(value)) == requested
            for value in values if value
        ):
            selected = candidate
            break

    address = selected.get("address")
    if not isinstance(address, dict):
        address = {}
    department = str(
        address.get("state")
        or address.get("region")
        or address.get("county")
        or ""
    )

    area_id = _overpass_area_id(selected)
    bbox = _nominatim_bbox(selected)
    cache = _load_overpass_cache()

    # Intento 1: área administrativa real.
    if area_id is not None:
        cache_key = (
            f"generic-area|{country_code.upper()}|{area_id}|"
            f"{normalize_text(city_name)}"
        )
        data = cache.get(cache_key)
        if not isinstance(data, dict):
            try:
                data = _overpass_request_with_fallback(
                    _overpass_postcode_query_for_area(area_id),
                    timeout,
                    ssl_mode,
                )
                cache[cache_key] = data
                _save_overpass_cache(cache)
            except ChallengeError as exc:
                logging.warning(
                    "[OSM] Overpass por área no pudo resolver %s:%s: %s",
                    city_name,
                    country_code,
                    exc,
                )
                data = {}

        locations = _locations_from_overpass_postcodes(
            data,
            city_name,
            country_code,
            department,
            bbox,
            "OpenStreetMap/Overpass (área)",
        )
        if locations:
            logging.info(
                "[OSM] %s:%s -> %d códigos postales por área",
                city_name,
                country_code,
                len(locations),
            )
            return locations

    # Intento 2: bbox de Nominatim. Este camino cubre ciudades cuyo resultado
    # es un nodo u otra entidad sin area_id derivable (caso típico internacional).
    if bbox is not None:
        south, west, north, east = bbox
        cache_key = (
            f"generic-bbox|{country_code.upper()}|{normalize_text(city_name)}|"
            f"{south:.5f},{west:.5f},{north:.5f},{east:.5f}"
        )
        data = cache.get(cache_key)
        if not isinstance(data, dict):
            try:
                data = _overpass_request_with_fallback(
                    _overpass_postcode_query_for_bbox(bbox),
                    timeout,
                    ssl_mode,
                )
                cache[cache_key] = data
                _save_overpass_cache(cache)
            except ChallengeError as exc:
                logging.warning(
                    "[OSM] Overpass por bbox no pudo resolver %s:%s: %s",
                    city_name,
                    country_code,
                    exc,
                )
                data = {}

        locations = _locations_from_overpass_postcodes(
            data,
            city_name,
            country_code,
            department,
            bbox,
            "OpenStreetMap/Overpass (bbox)",
        )
        if locations:
            logging.info(
                "[OSM] %s:%s -> %d códigos postales por bounding box",
                city_name,
                country_code,
                len(locations),
            )
            return locations

    logging.warning(
        "[OSM] %s:%s no produjo un conjunto postal utilizable por área ni bbox.",
        city_name,
        country_code,
    )
    return []


def expand_city_open_meteo_generic(
    city_name: str,
    country_code: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    params = {
        "name": city_name,
        "count": 20,
        "language": "es",
        "format": "json",
        "countryCode": country_code,
    }
    url = f"{GEOCODING_URL}?{urllib.parse.urlencode(params)}"

    try:
        data = http_get_json(url, timeout, ssl_mode)
    except ChallengeError:
        return []

    results = data.get("results")
    if not isinstance(results, list):
        return []

    wanted = normalize_text(city_name)
    candidate = None

    for item in results:
        if (
            str(item.get("country_code") or "").upper()
            != country_code.upper()
        ):
            continue

        if normalize_text(str(item.get("name") or "")) == wanted:
            candidate = item
            break

        if candidate is None:
            candidate = item

    if candidate is None:
        return []

    postcodes = candidate.get("postcodes")
    if not isinstance(postcodes, list):
        return []

    try:
        lat = float(candidate["latitude"])
        lon = float(candidate["longitude"])
    except (KeyError, TypeError, ValueError):
        return []

    department = str(candidate.get("admin1") or "")

    result: list[Location] = []

    for raw in postcodes:
        for postcode in _generic_postcode_values(
            raw,
            country_code,
        ):
            # Si solo existe una lista de códigos postales pero no una geometría separada
            # para cada zona, se resuelve cada código postal de forma independiente.
            try:
                loc = resolve_zip_online(
                    postcode,
                    country_code,
                    timeout,
                    ssl_mode,
                )
                result.append(
                    Location(
                        zip_code=postcode,
                        latitude=loc.latitude,
                        longitude=loc.longitude,
                        city=city_name,
                        department=loc.department or department,
                        name=f"{city_name} - ZIP {postcode}",
                        country_code=country_code.upper(),
                        coordinate_source=loc.coordinate_source,
                        minlon=loc.minlon,
                        minlat=loc.minlat,
                        maxlon=loc.maxlon,
                        maxlat=loc.maxlat,
                    )
                )
            except ChallengeError:
                pass

    unique, _ = unique_locations(result)
    return unique


def expand_city_final(
    city_spec: str,
    catalog: list[Location],
    geo_source: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    city_name, explicit_cc = parse_city_spec(city_spec)

    country_code = (
        explicit_cc
        or infer_city_country(
            city_name,
            catalog,
            timeout,
            ssl_mode,
        )
    ).upper()

    logging.info(
        "[CIUDAD] Resolviendo %s:%s",
        city_name,
        country_code,
    )

    # COLOMBIA: en modo automático se prefieren los polígonos oficiales de 4-72.
    if country_code == "CO" and geo_source in (
        "auto",
        "colombia472",
    ):
        expected_department = _expected_city_department(
            city_name,
            catalog,
        )

        try:
            locations = discover_colombia_city_postcodes_472(
                city_name,
                expected_department,
                timeout,
                ssl_mode,
            )
            if locations:
                return locations
        except ChallengeError as exc:
            if geo_source == "colombia472":
                raise
            logging.warning(
                "[4-72] No se pudo resolver %s con la "
                "fuente oficial: %s. Se intentará respaldo.",
                city_name,
                exc,
            )

    # INTERNACIONAL / ALTERNATIVA: área OpenStreet + Overpass.
    if geo_source in ("auto", "openstreet"):
        locations = discover_city_postcodes_openstreet_generic(
            city_name,
            country_code,
            timeout,
            ssl_mode,
        )
        if locations:
            return locations

    # Última alternativa de geocodificación si el proveedor publica códigos postales.
    if geo_source in ("auto", "openmeteo"):
        locations = expand_city_open_meteo_generic(
            city_name,
            country_code,
            timeout,
            ssl_mode,
        )
        if locations:
            return locations

    # El catálogo local aún puede cubrir exactamente la ciudad si se proporciona.
    catalog_matches = [
        loc for loc in catalog
        if normalize_text(loc.city) == normalize_text(city_name)
        and loc.country_code.upper() == country_code
    ]
    unique, _ = unique_locations(catalog_matches)

    if len(unique) > 1:
        return unique

    raise LocationResolutionError(
        f"No fue posible obtener un CONJUNTO de códigos postales "
        f"para '{city_name}:{country_code}'. "
        "No se sustituirá la ciudad por una sola coordenada. "
        "Use Ciudad:CC, --zips o un --catalog con cobertura completa."
    )



def expand_city_open_meteo(
    city_name: str,
    catalog: list[Location],
    geo_source: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Obtiene los postcodes que Open-Meteo/GeoNames publica para una ciudad.
    Si existen, cada ZIP se resuelve de forma independiente.
    """
    params = {
        "name": city_name,
        "count": 20,
        "language": "es",
        "format": "json",
        "countryCode": "CO",
    }
    url = f"{GEOCODING_URL}?{urllib.parse.urlencode(params)}"

    try:
        data = http_get_json(url, timeout, ssl_mode)
    except ChallengeError as exc:
        logging.warning(
            "No se pudo expandir '%s' con Open-Meteo Geocoding: %s",
            city_name,
            exc,
        )
        return []

    results = data.get("results")
    if not isinstance(results, list):
        return []

    requested_key = normalize_text(city_name)
    candidate = None

    for item in results:
        if str(item.get("country_code", "")).upper() != "CO":
            continue

        if normalize_text(str(item.get("name", ""))) == requested_key:
            candidate = item
            break

        if candidate is None:
            candidate = item

    if candidate is None:
        return []

    postcodes = candidate.get("postcodes") or []
    if not isinstance(postcodes, list):
        return []

    discovered = sorted(
        {
            code
            for raw in postcodes
            for code in _postcode_values(raw)
        }
    )

    resolved: list[Location] = []
    fallidos: list[str] = []

    for zip_code in discovered:
        try:
            location = resolve_zip_by_source(
                zip_code=zip_code,
                country_code="CO",
                catalog=catalog,
                geo_source=geo_source,
                timeout=timeout,
                ssl_mode=ssl_mode,
            )
            resolved.append(
                Location(
                    zip_code=location.zip_code,
                    latitude=location.latitude,
                    longitude=location.longitude,
                    city=city_name,
                    department=location.department,
                    name=f"{city_name} - ZIP {location.zip_code}",
                    country_code=location.country_code,
                    coordinate_source=location.coordinate_source,
                    minlon=location.minlon,
                    minlat=location.minlat,
                    maxlon=location.maxlon,
                    maxlat=location.maxlat,
                )
            )
        except ChallengeError:
            fallidos.append(zip_code)

    if fallidos:
        logging.warning(
            "Open-Meteo reportó ZIPs para %s que no pudieron resolverse: %s",
            city_name,
            ", ".join(fallidos),
        )

    return resolved


def _known_city_postcodes(city_name: str) -> tuple[str, ...]:
    return KNOWN_CITY_POSTCODES.get(
        normalize_text(city_name),
        (),
    )


def expand_city_complete(
    city_name: str,
    catalog: list[Location],
    geo_source: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    """
    Ciudad/municipio -> SET de ZIPs -> LAT/LON.

    Important invariant:
    a ZIP selected for a municipality MUST resolve inside the expected
    department. We never relabel an unrelated coordinate as if it belonged
    to the requested city.
    """
    key = normalize_text(city_name)
    expected_department = _expected_city_department(
        city_name,
        catalog,
    )

    catalog_matches = [
        loc for loc in catalog
        if normalize_text(loc.city) == key
        and _location_matches_expected_department(
            loc,
            expected_department,
        )
    ]

    osm_matches: list[Location] = []
    if geo_source in ("auto", "openstreet"):
        osm_matches = [
            loc
            for loc in discover_city_postcodes_openstreet(
                city_name,
                timeout,
                ssl_mode,
            )
            if _location_matches_expected_department(
                loc,
                expected_department,
            )
        ]

    open_meteo_matches: list[Location] = []
    if geo_source in ("auto", "openmeteo"):
        open_meteo_matches = [
            loc
            for loc in expand_city_open_meteo(
                city_name,
                catalog,
                geo_source,
                timeout,
                ssl_mode,
            )
            if _location_matches_expected_department(
                loc,
                expected_department,
            )
        ]

    combined = (
        catalog_matches
        + osm_matches
        + open_meteo_matches
    )

    known_codes = _known_city_postcodes(city_name)

    existing_codes = {
        loc.zip_code.strip()
        for loc in combined
    }

    missing_known = [
        code
        for code in known_codes
        if code not in existing_codes
    ]

    failed_known: list[str] = []

    for zip_code in missing_known:
        location: Location | None = None

        # Primera opción para una ciudad: consulta OSM estricta y consciente del municipio.
        if geo_source in ("auto", "openstreet"):
            try:
                location = resolve_zip_openstreet_in_city(
                    zip_code=zip_code,
                    city_name=city_name,
                    department=expected_department or None,
                    country_code="CO",
                    timeout=timeout,
                    ssl_mode=ssl_mode,
                )
            except ChallengeError as exc:
                logging.warning(
                    "[CIUDAD] ZIP OSM estricto %s falló: %s",
                    zip_code,
                    exc,
                )

        # Alternativa opcional: geocodificación Open-Meteo, pero SOLO si el resultado
        # todavía coincide con el departamento esperado.
        if location is None and geo_source in ("auto", "openmeteo"):
            try:
                candidate = resolve_zip_open_meteo(
                    zip_code,
                    "CO",
                    timeout,
                    ssl_mode,
                )
                if _location_matches_expected_department(
                    candidate,
                    expected_department,
                ):
                    location = candidate
            except ChallengeError:
                location = None

        if location is None:
            failed_known.append(zip_code)
            continue

        combined.append(
            Location(
                zip_code=location.zip_code,
                latitude=location.latitude,
                longitude=location.longitude,
                city=city_name,
                department=(
                    expected_department
                    or location.department
                ),
                name=f"{city_name} - ZIP {location.zip_code}",
                country_code=location.country_code,
                coordinate_source=location.coordinate_source,
                minlon=location.minlon,
                minlat=location.minlat,
                maxlon=location.maxlon,
                maxlat=location.maxlat,
            )
        )

    unique, _ = unique_locations(combined)

    # Validación defensiva final: ningún departamento incorrecto debe llegar a Open-Meteo.
    wrong_department = [
        loc
        for loc in unique
        if not _location_matches_expected_department(
            loc,
            expected_department,
        )
    ]

    if wrong_department:
        detail = ", ".join(
            f"{loc.zip_code}->{loc.department or 'UNKNOWN'}"
            for loc in wrong_department
        )
        raise LocationResolutionError(
            f"Se rechazaron ubicaciones fuera de "
            f"{expected_department or city_name}: {detail}"
        )

    if known_codes:
        present = {loc.zip_code for loc in unique}
        missing = [
            code for code in known_codes
            if code not in present
        ]

        if missing:
            raise LocationResolutionError(
                f"Se identificó el set de ZIPs de '{city_name}', pero "
                f"no fue posible validar LAT/LON dentro de "
                f"{expected_department or 'la ciudad'} para: "
                f"{', '.join(missing)}. "
                "No se usarán coordenadas de otro departamento."
            )

    if not unique:
        raise LocationResolutionError(
            f"No fue posible obtener un set de ZIPs geográficamente "
            f"válido para '{city_name}'."
        )

    logging.info(
        "[CIUDAD] %s -> %d ZIP(s) validated in %s: %s",
        city_name,
        len(unique),
        expected_department or "requested municipality",
        ", ".join(loc.zip_code for loc in unique),
    )

    return unique


def select_locations_from_catalog(
    catalog: list[Location],
    cities: list[str] | None,
    departments: list[str] | None,
    geo_source: str,
    timeout: float,
    ssl_mode: str,
) -> list[Location]:
    selected: list[Location] = []

    if cities:
        for requested in cities:
            selected.extend(
                expand_city_final(
                    requested,
                    catalog,
                    geo_source,
                    timeout,
                    ssl_mode,
                )
            )

    if departments:
        for requested in departments:
            selected.extend(
                expand_department_final(
                    requested,
                    catalog,
                    geo_source,
                    timeout,
                    ssl_mode,
                )
            )

    return selected


def resolve_requested_locations(
    args: argparse.Namespace,
) -> tuple[list[Location], int]:
    catalog = builtin_catalog()

    if args.catalog:
        catalog = merge_catalogs(
            catalog,
            load_external_catalog(Path(args.catalog)),
        )

    selected: list[Location] = []

    if args.zips:
        for spec in args.zips:
            zip_code, country_code = parse_zip_spec(spec)
            selected.append(
                resolve_zip_by_source(
                    zip_code=zip_code,
                    country_code=country_code,
                    catalog=catalog,
                    geo_source=args.geo_source,
                    timeout=args.timeout,
                    ssl_mode=args.ssl_mode,
                )
            )

    selected.extend(
        select_locations_from_catalog(
            catalog,
            args.cities,
            args.departments,
            args.geo_source,
            args.timeout,
            args.ssl_mode,
        )
    )

    if not selected:
        raise LocationResolutionError(
            "Debe indicar al menos una selección con --zips, --cities "
            "o --departments."
        )

    unique, removed = unique_locations(selected)

    # Si el docente desea observar explícitamente OpenStreet/cajas delimitadoras,
    # --geo-source openstreet enriquece también los ZIP obtenidos por
    # --cities/--departments. En automático no hacemos solicitudes OSM innecesarias.
    if args.geo_source != "openstreet":
        return unique, removed

    enriched: list[Location] = []

    for location in unique:
        if location.coordinate_source.startswith("OpenStreetMap/"):
            enriched.append(location)
            continue

        try:
            osm_location = resolve_zip_openstreet(
                location.zip_code,
                location.country_code,
                args.timeout,
                args.ssl_mode,
            )

            enriched.append(
                Location(
                    zip_code=osm_location.zip_code,
                    latitude=osm_location.latitude,
                    longitude=osm_location.longitude,
                    city=(
                        osm_location.city
                        if osm_location.city
                        and osm_location.city != location.zip_code
                        else location.city
                    ),
                    department=(
                        osm_location.department
                        if osm_location.department
                        else location.department
                    ),
                    name=osm_location.name or location.name,
                    country_code=osm_location.country_code,
                    coordinate_source=osm_location.coordinate_source,
                    minlon=osm_location.minlon,
                    minlat=osm_location.minlat,
                    maxlon=osm_location.maxlon,
                    maxlat=osm_location.maxlat,
                )
            )
        except ChallengeError as exc:
            logging.warning(
                "[OSM] No se pudo enriquecer ZIP %s: %s. "
                "Se conserva la resolución previa.",
                location.zip_code,
                exc,
            )
            enriched.append(location)

    enriched_unique, enriched_removed = unique_locations(enriched)
    return enriched_unique, removed + enriched_removed


# ---------------------------------------------------------------------------
# CONSULTA OPEN-METEO / NORMALIZACIÓN
# ---------------------------------------------------------------------------

def make_location_batches(
    locations: list[Location],
    batch_size: int,
) -> list[LocationBatch]:
    if batch_size <= 0:
        raise ValueError("batch_size debe ser > 0")

    return [
        LocationBatch(
            batch_id=f"B{index // batch_size + 1:03d}",
            locations=tuple(locations[index:index + batch_size]),
        )
        for index in range(0, len(locations), batch_size)
    ]


def fetch_solar_batch(
    batch: LocationBatch,
    start_date: str,
    end_date: str,
    timeout: float,
    ssl_mode: str,
    timezone: str,
) -> list[PipelineItem]:
    """
    Una request HTTP obtiene TODA la ventana temporal para varias coordenadas.

    Open-Meteo permite latitude/longitude separados por comas. Para varias
    ubicaciones el JSON cambia a una lista de estructuras, en el mismo orden
    de las coordenadas solicitadas.
    """
    if not batch.locations:
        return []

    params = {
        "latitude": ",".join(f"{loc.latitude:.7f}" for loc in batch.locations),
        "longitude": ",".join(f"{loc.longitude:.7f}" for loc in batch.locations),
        "start_date": start_date,
        "end_date": end_date,
        "hourly": ",".join(HOURLY_VARIABLES),
        "timezone": timezone,
    }

    # Con lotes conservadores la URL permanece manejable; las comas pueden
    # viajar codificadas y son interpretadas correctamente por el servidor.
    url = f"{ARCHIVE_URL}?{urllib.parse.urlencode(params)}"
    data = _http_get_json_value(url, timeout, ssl_mode)

    if batch.size == 1 and isinstance(data, dict):
        payloads = [data]
    elif isinstance(data, list):
        payloads = data
    else:
        raise APIRequestError(
            f"{batch.batch_id}: respuesta multi-location inesperada "
            f"para {batch.size} sitio(s)."
        )

    if len(payloads) != batch.size:
        raise DataValidationError(
            f"{batch.batch_id}: Open-Meteo devolvió {len(payloads)} estructura(s) "
            f"para {batch.size} coordenada(s)."
        )

    items: list[PipelineItem] = []
    for location, payload in zip(batch.locations, payloads):
        if not isinstance(payload, dict):
            raise DataValidationError(
                f"{batch.batch_id}: payload inválido para ZIP {location.zip_code}."
            )
        if payload.get("error"):
            raise APIRequestError(
                f"{batch.batch_id}/ZIP {location.zip_code}: "
                f"{payload.get('reason', 'Error de API')}"
            )
        items.append(PipelineItem(location=location, payload=payload))

    return items


def classify_transient_api_error(exc: APIRequestError) -> str | None:
    """Clasifica un fallo únicamente cuando puede ser transitorio."""
    message = str(exc).lower()

    if getattr(exc, "status_code", None) == 429 or "http 429" in message:
        return "http_429"

    http_match = re.search(r"http\s+(\d{3})", message)
    if http_match:
        status = int(http_match.group(1))
        if 500 <= status <= 599:
            return "http_5xx"

    if (
        "timeout" in message
        or "timed out" in message
        or "tiempo de espera agotado" in message
    ):
        return "timeout"

    if "error de red" in message:
        return "network"

    return None


class RateLimitCoordinator:
    """Coordina un único enfriamiento global cuando Open-Meteo responde 429."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._blocked_until = 0.0
        self._wait_events = 0
        self._wait_seconds = 0.0

    def _remaining(self) -> float:
        with self._lock:
            return max(0.0, self._blocked_until - time.monotonic())

    def wait_if_needed(self, requester_name: str, batch_label: str) -> None:
        remaining = self._remaining()
        if remaining <= 0.0:
            return
        logging.warning(
            "[%s] %s: rate limit activo -> espera externa %.1f s antes de HTTP",
            requester_name,
            batch_label,
            remaining,
        )
        started = time.monotonic()
        threading.Event().wait(remaining)
        waited = time.monotonic() - started
        with self._lock:
            self._wait_events += 1
            self._wait_seconds += waited

    def register_429(
        self,
        exc: APIRequestError,
        requester_name: str,
        batch_label: str,
    ) -> None:
        delay = getattr(exc, "retry_after", None)
        if delay is None or delay <= 0.0:
            delay = DEFAULT_429_BACKOFF_SECONDS

        now = time.monotonic()
        with self._lock:
            if self._blocked_until <= now:
                self._blocked_until = now + delay
            remaining = max(0.0, self._blocked_until - now)

        logging.warning(
            "[%s] %s: HTTP 429 -> enfriamiento global %.1f s ",
            requester_name,
            batch_label,
            remaining,
        )

    def stats(self) -> dict[str, float | int]:
        with self._lock:
            return {
                "esperas_rate_limit": self._wait_events,
                "segundos_espera_rate_limit": self._wait_seconds,
            }


def fetch_solar_batch_resilient(
    batch: LocationBatch,
    start_date: str,
    end_date: str,
    timeout: float,
    ssl_mode: str,
    timezone: str,
    max_attempts: int,
    request_stats: RequestStatsCollector,
    requester_name: str,
    api_gate: APIConcurrencyGate | None = None,
    rate_limit: RateLimitCoordinator | None = None,
) -> list[PipelineItem]:
    """
    Ejecuta una solicitud multi-location con reintentos acotados y visibles.

    Un intento HTTP representa un lote completo, no una petición por hora/día.
    Secuencial y paralelo usan exactamente la misma granularidad de lotes.
    """
    request_stats.begin_request()
    last_error: APIRequestError | None = None

    for attempt in range(1, max_attempts + 1):
        if rate_limit is not None:
            rate_limit.wait_if_needed(requester_name, batch.label)

        request_stats.record_attempt()

        if api_gate is not None:
            api_gate.acquire(requester_name, batch.label)

        try:
            items = fetch_solar_batch(
                batch,
                start_date,
                end_date,
                timeout,
                ssl_mode,
                timezone,
            )

        except APIRequestError as exc:
            last_error = exc
            category = classify_transient_api_error(exc)

            if category is not None and attempt < max_attempts:
                request_stats.record_retry(category)
                if category == "http_429" and rate_limit is not None:
                    rate_limit.register_429(exc, requester_name, batch.label)
                logging.warning(
                    "[%s] %s: fallo transitorio intento %d/%d: %s -> REINTENTO",
                    requester_name,
                    batch.label,
                    attempt,
                    max_attempts,
                    exc,
                )
                continue

            request_stats.record_final_failure(category)
            raise

        finally:
            if api_gate is not None:
                api_gate.release()

        if attempt > 1:
            request_stats.record_recovered()
            logging.info(
                "[%s] %s recuperado correctamente en intento %d/%d",
                requester_name,
                batch.label,
                attempt,
                max_attempts,
            )

        return items

    if last_error is not None:
        raise last_error

    raise APIRequestError(f"{batch.batch_id}: solicitud sin resultado.")


def _timestamp_hour(timestamp: Any) -> int:
    text = str(timestamp)
    match = re.search(r"T(\d{2}):", text)
    if not match:
        raise DataValidationError(
            f"Timestamp horario inesperado: {timestamp!r}"
        )
    return int(match.group(1))


def normalize_item(
    item: PipelineItem,
    start_hour: int | None = None,
    end_hour: int | None = None,
) -> SiteSeries:
    hourly = item.payload.get("hourly")
    if not isinstance(hourly, dict):
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: respuesta sin 'hourly'."
        )

    required = ("time",) + HOURLY_VARIABLES
    missing = [name for name in required if name not in hourly]

    if missing:
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: faltan {missing}."
        )

    arrays = [hourly[name] for name in required]

    if not all(isinstance(values, list) for values in arrays):
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: hourly no contiene listas válidas."
        )

    lengths = {len(values) for values in arrays}
    if len(lengths) != 1:
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: longitudes horarias inconsistentes."
        )

    rows: list[dict[str, Any]] = []

    for i, timestamp in enumerate(hourly["time"]):
        if start_hour is not None:
            hour = _timestamp_hour(timestamp)
            if not start_hour <= hour <= end_hour:
                continue

        row = {"datetime": timestamp}
        for variable in HOURLY_VARIABLES:
            value = hourly[variable][i]
            if value is None:
                raise DataValidationError(
                    f"ZIP {item.location.zip_code}: valor nulo en "
                    f"{variable} para {timestamp}."
                )
            row[variable] = value
        rows.append(row)

    if not rows:
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: la ventana horaria seleccionada "
            "no produjo registros."
        )

    return SiteSeries(
        location=item.location,
        rows=tuple(rows),
    )


# ---------------------------------------------------------------------------
# COLECTORES SEGUROS PARA HILOS
# ---------------------------------------------------------------------------

class ErrorCollector:
    def __init__(self) -> None:
        self._errors: list[str] = []
        self._lock = threading.Lock()

    def add(self, message: str) -> None:
        with self._lock:
            self._errors.append(message)

    def snapshot(self) -> list[str]:
        with self._lock:
            return list(self._errors)


class RequestStatsCollector:
    """Acumula de forma segura métricas de solicitudes y reintentos."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._logical_requests = 0
        self._http_attempts = 0
        self._retries = 0
        self._recovered = 0
        self._final_failures = 0
        self._http_429 = 0
        self._http_5xx = 0
        self._timeouts = 0
        self._network_errors = 0

    def begin_request(self) -> None:
        with self._lock:
            self._logical_requests += 1

    def record_attempt(self) -> None:
        with self._lock:
            self._http_attempts += 1

    def _record_category_unlocked(self, category: str) -> None:
        if category == "http_429":
            self._http_429 += 1
        elif category == "http_5xx":
            self._http_5xx += 1
        elif category == "timeout":
            self._timeouts += 1
        elif category == "network":
            self._network_errors += 1

    def record_retry(self, category: str) -> None:
        with self._lock:
            self._retries += 1
            self._record_category_unlocked(category)

    def record_recovered(self) -> None:
        with self._lock:
            self._recovered += 1

    def record_final_failure(self, category: str | None) -> None:
        with self._lock:
            self._final_failures += 1
            if category is not None:
                self._record_category_unlocked(category)

    def snapshot(self) -> dict[str, int]:
        with self._lock:
            return {
                "solicitudes_logicas": self._logical_requests,
                "intentos_http": self._http_attempts,
                "reintentos": self._retries,
                "solicitudes_recuperadas": self._recovered,
                "fallos_definitivos": self._final_failures,
                "eventos_http_429": self._http_429,
                "eventos_http_5xx": self._http_5xx,
                "eventos_timeout": self._timeouts,
                "eventos_red": self._network_errors,
            }


class APIConcurrencyGate:
    """
    Limita únicamente la concurrencia contra Open-Meteo.

    Los productores siguen siendo hilos independientes y el búfer continúa
    sincronizado mediante Condition Variables. Esta puerta protege el recurso
    externo sin cambiar la lógica Productor-Consumidor.
    """

    def __init__(self, capacity: int) -> None:
        if capacity <= 0:
            raise ValueError("La concurrencia de API debe ser > 0.")

        self._capacity = capacity
        self._semaphore = threading.BoundedSemaphore(capacity)
        self._lock = threading.Lock()
        self._active = 0
        self._max_active = 0
        self._waits = 0

    @property
    def capacity(self) -> int:
        return self._capacity

    def acquire(self, worker_name: str, zip_code: str) -> None:
        acquired = self._semaphore.acquire(blocking=False)

        if not acquired:
            with self._lock:
                self._waits += 1
            logging.info(
                "[%s] Límite API ocupado (%d simultáneas) para ZIP %s -> ESPERA",
                worker_name,
                self._capacity,
                zip_code,
            )
            self._semaphore.acquire()

        with self._lock:
            self._active += 1
            self._max_active = max(self._max_active, self._active)

    def release(self) -> None:
        with self._lock:
            self._active -= 1
        self._semaphore.release()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "limite_concurrencia_api": self._capacity,
                "esperas_limite_api": self._waits,
                "concurrencia_api_max_observada": self._max_active,
            }


class SeriesCollector:
    """
    Almacena una serie por sitio.

    Este NO hace aún la agrupación solar; esa agrupación se realiza
    comparando la serie completa de cada sitio al terminar el pipeline.
    """

    def __init__(self) -> None:
        self._series: dict[
            tuple[str, float, float],
            SiteSeries,
        ] = {}
        self._duplicate_site_inputs = 0
        self._lock = threading.Lock()

    def add(self, series: SiteSeries) -> None:
        key = series.location.identity

        with self._lock:
            if key in self._series:
                self._duplicate_site_inputs += 1
                return
            self._series[key] = series

    def snapshot(self) -> list[SiteSeries]:
        with self._lock:
            return list(self._series.values())

    @property
    def duplicate_site_inputs(self) -> int:
        with self._lock:
            return self._duplicate_site_inputs


# ---------------------------------------------------------------------------
# VARIABLES DE CONDICIÓN / BÚFER ACOTADO
# ---------------------------------------------------------------------------

class BoundedBuffer:
    """
    Buffer acotado implementado MANUALMENTE con dos Condition Variables
    que comparten el mismo Lock.

    - not_full: productores esperan cuando no existe espacio.
    - not_empty: consumidores esperan cuando no existen datos.

    Usar dos condiciones evita despertar hilos que no pueden progresar.
    En ambos casos wait() se mantiene dentro de while para volver a comprobar
    el estado al despertar.

    Cuando active_producers == 0 y el buffer está vacío, get() retorna None
    y permite el cierre ordenado.
    """

    def __init__(self, capacity: int, active_producers: int) -> None:
        if capacity <= 0:
            raise ValueError("capacity debe ser > 0")
        if active_producers <= 0:
            raise ValueError("active_producers debe ser > 0")

        self._buffer: deque[PipelineItem] = deque()
        self._capacity = capacity
        self._active_producers = active_producers

        self._lock = threading.Lock()
        self._not_empty = threading.Condition(self._lock)
        self._not_full = threading.Condition(self._lock)

        # Evidencia cuantitativa de sincronización.
        self._producer_waits = 0
        self._consumer_waits = 0
        self._producer_wakeups = 0
        self._consumer_wakeups = 0
        self._pushes = 0
        self._pops = 0
        self._signals_to_consumers = 0
        self._signals_to_producers = 0
        self._notify_all_shutdowns = 0
        self._producers_finished = 0
        self._consumer_shutdowns = 0
        self._max_occupancy = 0

    def put(self, item: PipelineItem, producer_name: str) -> None:
        with self._not_full:
            while len(self._buffer) >= self._capacity:
                self._producer_waits += 1
                logging.info(
                    "[%s] Buffer LLENO (%d/%d) -> ESPERA",
                    producer_name,
                    len(self._buffer),
                    self._capacity,
                )
                self._not_full.wait()
                self._producer_wakeups += 1
                logging.info(
                    "[%s] DESPIERTA -> revalidar espacio disponible",
                    producer_name,
                )

            self._buffer.append(item)
            self._pushes += 1
            self._max_occupancy = max(
                self._max_occupancy,
                len(self._buffer),
            )

            logging.info(
                "[%s] INSERTA ZIP %s (%d/%d) -> SEÑAL al consumidor",
                producer_name,
                item.location.zip_code,
                len(self._buffer),
                self._capacity,
            )

            # Se despierta un consumidor: ahora existe al menos un elemento.
            self._signals_to_consumers += 1
            self._not_empty.notify()

    def get(self, consumer_name: str) -> PipelineItem | None:
        with self._not_empty:
            while not self._buffer and self._active_producers > 0:
                self._consumer_waits += 1
                logging.info(
                    "[%s] Buffer VACÍO; productores activos=%d -> ESPERA",
                    consumer_name,
                    self._active_producers,
                )
                self._not_empty.wait()
                self._consumer_wakeups += 1
                logging.info(
                    "[%s] DESPIERTA -> revalidar disponibilidad de datos",
                    consumer_name,
                )

            if not self._buffer and self._active_producers == 0:
                self._consumer_shutdowns += 1
                logging.info(
                    "[%s] Buffer vacío + sin productores activos -> "
                    "cierre ordenado",
                    consumer_name,
                )
                return None

            item = self._buffer.popleft()
            self._pops += 1

            logging.info(
                "[%s] EXTRAE ZIP %s (%d/%d) -> SEÑAL al productor",
                consumer_name,
                item.location.zip_code,
                len(self._buffer),
                self._capacity,
            )

            # Se despierta un productor: ahora existe al menos un espacio.
            self._signals_to_producers += 1
            self._not_full.notify()
            return item

    def producer_done(self, producer_name: str) -> None:
        with self._not_empty:
            if self._active_producers <= 0:
                raise RuntimeError(
                    "producer_done() llamado más veces de las esperadas."
                )

            self._active_producers -= 1
            self._producers_finished += 1

            logging.info(
                "[%s] Finalizó; productores activos=%d",
                producer_name,
                self._active_producers,
            )

            if self._active_producers == 0:
                logging.info(
                    "[%s] Último productor -> SEÑAL A TODOS los consumidores para cierre",
                    producer_name,
                )
                # Los consumidores con el búfer vacío deben despertar y detectar que
                # ya no habrá más datos.
                self._notify_all_shutdowns += 1
                self._not_empty.notify_all()

    def stats(self) -> dict[str, int]:
        """Copia consistente de las métricas de sincronización."""
        with self._lock:
            return {
                "esperas_productores_buffer_lleno": self._producer_waits,
                "esperas_consumidores_buffer_vacio": self._consumer_waits,
                "despertares_productores": self._producer_wakeups,
                "despertares_consumidores": self._consumer_wakeups,
                "elementos_insertados": self._pushes,
                "elementos_extraidos": self._pops,
                "senales_a_consumidores": self._signals_to_consumers,
                "senales_a_productores": self._signals_to_producers,
                "notify_all_cierre": self._notify_all_shutdowns,
                "productores_finalizados": self._producers_finished,
                "consumidores_cerrados": self._consumer_shutdowns,
                "ocupacion_maxima_buffer": self._max_occupancy,
            }


# ---------------------------------------------------------------------------
# DEDUPLICACIÓN / AGRUPACIÓN DE LA SERIE METEOROLÓGICA COMPLETA
# ---------------------------------------------------------------------------

def canonical_weather_value(value: Any) -> Any:
    """
    Conserva igualdad EXACTA de los datos solares devueltos.

    No se usa tolerancia ni redondeo para decidir que dos series son iguales.
    """
    return value


def weather_signature(series: SiteSeries) -> tuple[Any, ...]:
    """
    Firma de TODA la ventana temporal del sitio.

    Dos sitios se agrupan únicamente si tienen exactamente la misma secuencia
    horaria (datetime + las 12 variables de radiación solar) durante toda la ventana.
    """
    return tuple(
        (
            row["datetime"],
            *(canonical_weather_value(row[name]) for name in HOURLY_VARIABLES),
        )
        for row in series.rows
    )


def group_identical_weather_series(
    series_list: list[SiteSeries],
) -> list[WeatherGroup]:
    buckets: dict[tuple[Any, ...], list[SiteSeries]] = defaultdict(list)

    for series in series_list:
        buckets[weather_signature(series)].append(series)

    provisional: list[
        tuple[Location, tuple[Location, ...], tuple[dict[str, Any], ...]]
    ] = []

    for members_series in buckets.values():
        members_series.sort(
            key=lambda s: (
                normalize_text(s.location.department),
                normalize_text(s.location.city),
                s.location.zip_code,
                s.location.latitude,
                s.location.longitude,
            )
        )

        representative_series = members_series[0]
        members = tuple(s.location for s in members_series)

        provisional.append(
            (
                representative_series.location,
                members,
                representative_series.rows,
            )
        )

    provisional.sort(
        key=lambda item: (
            normalize_text(item[0].department),
            normalize_text(item[0].city),
            item[0].zip_code,
            item[0].latitude,
            item[0].longitude,
        )
    )

    groups: list[WeatherGroup] = []

    for index, (representative, members, rows) in enumerate(
        provisional,
        start=1,
    ):
        groups.append(
            WeatherGroup(
                group_id=f"G{index:03d}",
                representative=representative,
                members=members,
                rows=rows,
            )
        )

    return groups


def groups_to_final_rows(
    groups: list[WeatherGroup],
) -> list[dict[str, Any]]:
    """
    Construye el CSV final agrupado y legible.

    Regla:
      - dos ZIP diferentes se agrupan SOLO si toda su secuencia
        datetime + 12 variables solares es exactamente idéntica;
      - la fila final conserva UN zipcode representativo de 6 dígitos;
      - zipcode sigue siendo str en todo momento;
      - la lista completa de miembros del grupo NO se mete en una celda:
        se conserva en memoria y se reporta en la consola.
    """
    rows: list[dict[str, Any]] = []

    for group in groups:
        representative_zip = str(group.representative.zip_code)

        if group.representative.country_code.upper() == "CO":
            if not re.fullmatch(r"\d{6}", representative_zip):
                raise DataValidationError(
                    f"ZIP colombiano representativo inválido: {representative_zip!r}"
                )

        for weather in group.rows:
            row: dict[str, Any] = {
                "zipcode": representative_zip,
                "datetime": weather["datetime"],
                "representative_latitude": group.representative.latitude,
                "representative_longitude": group.representative.longitude,
            }
            for variable in HOURLY_VARIABLES:
                row[variable] = weather[variable]
            rows.append(row)

    rows.sort(
        key=lambda row: (
            row["zipcode"],
            row["datetime"],
        )
    )
    return rows

def groups_to_summary_rows(
    groups: list[WeatherGroup],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    for group in groups:
        members = list(group.members)

        rows.append(
            {
                "group_id": group.group_id,
                "representative_zip": group.representative.zip_code,
                "representative_site": group.representative.name,
                "representative_latitude": group.representative.latitude,
                "representative_longitude": group.representative.longitude,
                "coordinate_source": group.representative.coordinate_source,
                "minlon": group.representative.minlon,
                "minlat": group.representative.minlat,
                "maxlon": group.representative.maxlon,
                "maxlat": group.representative.maxlat,
                "member_count": len(members),
                "grouped_zip_codes": join_semicolon(
                    sorted({m.zip_code for m in members})
                ),
                "grouped_sites": join_semicolon(
                    f"{m.city}/{m.department}/ZIP {m.zip_code}"
                    for m in members
                ),
                "grouped_coordinates": join_semicolon(
                    coordinate_text(m) for m in members
                ),
                "same_weather_series": "SÍ" if len(members) > 1 else "ÚNICA",
            }
        )

    return rows


# ---------------------------------------------------------------------------
# VERSIÓN SECUENCIAL
# ---------------------------------------------------------------------------

def run_serial(
    locations: list[Location],
    start_date: str,
    end_date: str,
    timeout: float,
    ssl_mode: str,
    timezone: str = "auto",
    start_hour: int | None = None,
    end_hour: int | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> tuple[
    list[WeatherGroup],
    list[dict[str, Any]],
    int,
    float,
    list[str],
    dict[str, int],
]:
    collector = SeriesCollector()
    errors: list[str] = []
    request_stats = RequestStatsCollector()
    rate_limit = RateLimitCoordinator()
    batches = make_location_batches(locations, batch_size)

    started = time.perf_counter()

    for batch in batches:
        logging.info(
            "[SECUENCIAL] Solicitando %s | %s",
            batch.label,
            ",".join(loc.zip_code for loc in batch.locations),
        )

        try:
            items = fetch_solar_batch_resilient(
                batch=batch,
                start_date=start_date,
                end_date=end_date,
                timeout=timeout,
                ssl_mode=ssl_mode,
                timezone=timezone,
                max_attempts=max_attempts,
                request_stats=request_stats,
                requester_name="SECUENCIAL",
                rate_limit=rate_limit,
            )

            for item in items:
                series = normalize_item(item, start_hour, end_hour)
                collector.add(series)
                logging.info(
                    "[SECUENCIAL] ZIP %s procesado -> %d filas horarias",
                    item.location.zip_code,
                    len(series.rows),
                )

        except ChallengeError as exc:
            zips = ",".join(loc.zip_code for loc in batch.locations)
            message = f"SECUENCIAL {batch.batch_id} [{zips}]: {exc}"
            logging.error(message)
            errors.append(message)

    groups = group_identical_weather_series(collector.snapshot())
    final_rows = groups_to_final_rows(groups)
    elapsed = time.perf_counter() - started

    return (
        groups,
        final_rows,
        collector.duplicate_site_inputs,
        elapsed,
        errors,
        {**request_stats.snapshot(), **rate_limit.stats()},
    )

# ---------------------------------------------------------------------------
# VERSIÓN PARALELA PRODUCTOR-CONSUMIDOR
# ---------------------------------------------------------------------------

class JobAllocator:
    """
    Cola de lotes independiente del número de workers.

    La partición se define por ``batch_size`` y el volumen geográfico, no por
    ``os.cpu_count()``. Los productores toman lotes dinámicamente.
    """

    def __init__(self, batches: list[LocationBatch]) -> None:
        self._pending: deque[LocationBatch] = deque(batches)
        self._lock = threading.Lock()
        self._total = len(batches)
        self._sites = sum(batch.size for batch in batches)
        self._assigned = 0

    def get_next(self) -> LocationBatch | None:
        with self._lock:
            if not self._pending:
                return None
            self._assigned += 1
            return self._pending.popleft()

    def stats(self) -> dict[str, int]:
        with self._lock:
            return {
                "tareas_logicas_totales": self._total,
                "tareas_asignadas": self._assigned,
                "tareas_pendientes": len(self._pending),
                "sitios_en_tareas": self._sites,
            }


def producer_worker(
    producer_id: int,
    allocator: JobAllocator,
    buffer: BoundedBuffer,
    start_date: str,
    end_date: str,
    timeout: float,
    ssl_mode: str,
    timezone: str,
    errors: ErrorCollector,
    api_gate: APIConcurrencyGate,
    rate_limit: RateLimitCoordinator,
    request_stats: RequestStatsCollector,
    max_attempts: int,
) -> None:
    name = f"Productor-{producer_id}"

    try:
        while True:
            batch = allocator.get_next()
            if batch is None:
                break

            logging.info(
                "[%s] TOMA TAREA %s | %s",
                name,
                batch.label,
                ",".join(loc.zip_code for loc in batch.locations),
            )

            try:
                items = fetch_solar_batch_resilient(
                    batch=batch,
                    start_date=start_date,
                    end_date=end_date,
                    timeout=timeout,
                    ssl_mode=ssl_mode,
                    timezone=timezone,
                    max_attempts=max_attempts,
                    request_stats=request_stats,
                    requester_name=name,
                    api_gate=api_gate,
                    rate_limit=rate_limit,
                )
            except ChallengeError as exc:
                zips = ",".join(loc.zip_code for loc in batch.locations)
                message = f"{name} {batch.batch_id} [{zips}]: {exc}"
                logging.error("[%s] %s", name, exc)
                errors.add(message)
                continue

            # Una request puede producir varios elementos. Cada sitio entra al
            # búfer acotado individualmente para conservar el patrón
            # Productor-Consumidor y la evidencia wait/notify.
            for item in items:
                buffer.put(item, name)

    finally:
        buffer.producer_done(name)

def consumer_worker(
    consumer_id: int,
    buffer: BoundedBuffer,
    collector: SeriesCollector,
    errors: ErrorCollector,
    start_hour: int | None,
    end_hour: int | None,
) -> None:
    name = f"Consumidor-{consumer_id}"

    while True:
        item = buffer.get(name)

        if item is None:
            return

        try:
            series = normalize_item(
                item,
                start_hour,
                end_hour,
            )

            logging.info(
                "[%s] PROCESA ZIP %s -> %d filas horarias",
                name,
                item.location.zip_code,
                len(series.rows),
            )

            collector.add(series)

        except ChallengeError as exc:
            message = f"{name} ZIP {item.location.zip_code}: {exc}"
            logging.error("[%s] %s", name, exc)
            errors.add(message)


def run_parallel(
    locations: list[Location],
    start_date: str,
    end_date: str,
    producer_count: int,
    consumer_count: int,
    buffer_size: int,
    timeout: float,
    ssl_mode: str,
    timezone: str = "auto",
    start_hour: int | None = None,
    end_hour: int | None = None,
    api_concurrency: int = DEFAULT_API_CONCURRENCY,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    batch_size: int = DEFAULT_BATCH_SIZE,
) -> tuple[
    list[WeatherGroup],
    list[dict[str, Any]],
    int,
    float,
    list[str],
    dict[str, int],
    dict[str, int],
]:
    collector = SeriesCollector()
    errors = ErrorCollector()
    request_stats = RequestStatsCollector()

    effective_api_concurrency = max(1, min(api_concurrency, producer_count))
    api_gate = APIConcurrencyGate(effective_api_concurrency)
    rate_limit = RateLimitCoordinator()

    buffer = BoundedBuffer(
        capacity=buffer_size,
        active_producers=producer_count,
    )

    batches = make_location_batches(locations, batch_size)
    allocator = JobAllocator(batches)

    consumers = [
        threading.Thread(
            target=consumer_worker,
            args=(
                index + 1,
                buffer,
                collector,
                errors,
                start_hour,
                end_hour,
            ),
            name=f"Consumidor-{index + 1}",
        )
        for index in range(consumer_count)
    ]

    producers = [
        threading.Thread(
            target=producer_worker,
            args=(
                index + 1,
                allocator,
                buffer,
                start_date,
                end_date,
                timeout,
                ssl_mode,
                timezone,
                errors,
                api_gate,
                rate_limit,
                request_stats,
                max_attempts,
            ),
            name=f"Productor-{index + 1}",
        )
        for index in range(producer_count)
    ]

    started = time.perf_counter()

    # Los consumidores se inician primero para que VACÍO -> ESPERA pueda
    # observarse de forma natural en la evidencia de sincronización.
    for thread in consumers:
        thread.start()

    for thread in producers:
        thread.start()

    # Requisito crítico: esperar a TODOS los hilos antes de aceptar tiempos,
    # validar resultados o escribir archivos.
    for thread in producers:
        thread.join()

    for thread in consumers:
        thread.join()

    groups = group_identical_weather_series(collector.snapshot())
    final_rows = groups_to_final_rows(groups)
    elapsed = time.perf_counter() - started

    sync_stats = buffer.stats()
    sync_stats["hilos_join_realizados"] = len(producers) + len(consumers)
    sync_stats.update(allocator.stats())
    sync_stats["tamano_lote_configurado"] = batch_size
    sync_stats["lotes_reales"] = len(batches)
    sync_stats.update(api_gate.stats())

    return (
        groups,
        final_rows,
        collector.duplicate_site_inputs,
        elapsed,
        errors.snapshot(),
        sync_stats,
        {**request_stats.snapshot(), **rate_limit.stats()},
    )


# ---------------------------------------------------------------------------
# VALIDACIÓN Y SALIDA
# ---------------------------------------------------------------------------

def canonical_final_rows(
    rows: list[dict[str, Any]],
) -> list[tuple[Any, ...]]:
    canonical = [
        tuple(row[field] for field in FINAL_CSV_FIELDS)
        for row in rows
    ]
    canonical.sort()
    return canonical


def canonical_group_summary(
    groups: list[WeatherGroup],
) -> list[tuple[Any, ...]]:
    summary: list[tuple[Any, ...]] = []

    for group in groups:
        members = tuple(
            sorted(
                (
                    m.zip_code,
                    round(m.latitude, 7),
                    round(m.longitude, 7),
                    normalize_text(m.city),
                    normalize_text(m.department),
                )
                for m in group.members
            )
        )

        summary.append(
            (
                group.group_id,
                group.representative.zip_code,
                members,
                weather_signature(
                    SiteSeries(
                        location=group.representative,
                        rows=group.rows,
                    )
                ),
            )
        )

    summary.sort()
    return summary


def write_csv(
    path: Path,
    fieldnames: tuple[str, ...],
    rows: list[dict[str, Any]],
    delimiter: str = ",",
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open(
        "w",
        newline="",
        encoding="utf-8-sig",
    ) as file:
        writer = csv.DictWriter(
            file,
            fieldnames=fieldnames,
            delimiter=delimiter,
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        writer.writerows(rows)


def groups_output_path(output_path: Path) -> Path:
    suffix = output_path.suffix or ".csv"
    stem = output_path.stem
    return output_path.with_name(
        f"{stem}_groups{suffix}"
    )


def count_grouped_redundant_sites(
    groups: list[WeatherGroup],
) -> int:
    return sum(
        max(0, len(group.members) - 1)
        for group in groups
    )


def print_grouping_summary(
    groups: list[WeatherGroup],
    input_sites: int,
) -> None:
    print_separator("AGRUPACIÓN DE SERIES SOLARES / CONTROL DE DUPLICADOS")

    redundant = count_grouped_redundant_sites(groups)

    print(f"Sitios de entrada:           {input_sites}")
    print(f"Series solares únicas:      {len(groups):>3}")
    print(f"Sitios redundantes agrupados:{redundant:>3}")

    for group in groups:
        if len(group.members) <= 1:
            continue

        print()
        print(
            f"{group.group_id}: {len(group.members)} sitios con "
            "la MISMA serie solar"
        )

        for member in group.members:
            print(
                f"  - ZIP {member.zip_code} | "
                f"{member.city}, {member.department} | "
                f"({member.latitude:.6f}, {member.longitude:.6f})"
            )


# ---------------------------------------------------------------------------
# INTERFAZ DE LÍNEA DE COMANDOS
# ---------------------------------------------------------------------------

class SpanishArgumentParser(argparse.ArgumentParser):
    """ArgumentParser con etiquetas visibles en español."""

    def format_help(self) -> str:
        salida = super().format_help()
        for origen, destino in {
            "usage:": "Uso:",
            "options:": "Opciones:",
            "show this help message and exit":
                "muestra esta ayuda y termina",
            "show program's version number and exit":
                "muestra la versión del programa y termina",
            "(default:": "(predeterminado:",
        }.items():
            salida = salida.replace(origen, destino)
        return salida


class ChallengeHelpFormatter(
    argparse.ArgumentDefaultsHelpFormatter,
    argparse.RawDescriptionHelpFormatter,
):
    pass


def build_parser() -> argparse.ArgumentParser:
    parser = SpanishArgumentParser(
        description=(
            "Challenge 7 - Canalización Productor-Consumidor con "
            "Variables de Condición"
        ),
        epilog=r"""
EJECUCIÓN RÁPIDA

Sin argumentos, `python chal.py` abre un modo interactivo guiado.
`python chal.py --help` muestra esta ayuda.

EJEMPLOS

1) Conjunto explícito de ZIPs:
   python chal.py --zips 130001:CO 110111:CO --start 2026-09-01 --end 2026-09-03 --buffer-size 1

2) Ciudad/municipio:
   python chal.py --cities Cartagena --start 2026-09-01 --end 2026-09-03 --buffer-size 1

3) Recuperación - plan seguro para un departamento y un mes (sin Open-Meteo):
   python chal.py --departments Antioquia --start 2026-08-01 --end 2026-08-31 --plan-only

4) Departamento colombiano automático con 4-72 / MinTIC:
   python chal.py --departments Antioquia --start 2026-08-01 --end 2026-08-31

5) Forzar OpenStreet/Nominatim para ver LAT/LON + caja delimitadora:
   python chal.py --zips 130001:CO --geo-source openstreet --start 2026-09-01 --end 2026-09-03

6) Ciudad + ventana horaria local:
   python chal.py --cities Cartagena --start 2026-09-01 --end 2026-09-03 --start-hour 08 --end-hour 16

7) Varias ciudades y países:
   python chal.py --cities Cartagena Medellin Lima:PE --start 2026-09-01 --end 2026-09-03

En Linux/macOS puede usar `python3 chal.py` en lugar de `python chal.py`.

POLÍTICA DE WORKERS - REGLA DEL DOCENTE
- El modo paralelo detecta el presupuesto con os.cpu_count().
- Productores + consumidores = EXACTAMENTE todos los núcleos lógicos detectados.
- El conteo NO depende del número de ZIP/sitios/trabajos.
- El conteo NO es configurable por línea de comandos.

ALCANCE DEL CRONOMETRAJE
- La resolución geográfica ZIP/ciudad/departamento se hace ANTES de la medición.
- Tsecuencial y Tparalelo miden exactamente la misma carga de trabajo solar:
  mismos ZIP/LAT/LON + mismas fechas + mismas horas + mismas variables.
  Incluye consultas Open-Meteo + normalización + agrupación exacta de series solares.
- La escritura final del CSV ocurre DESPUÉS de la medición.
- Por defecto se realiza 1 ejecución por modalidad; --repetitions permite repetir el experimento cuando se requiera análisis estadístico.
- Aceleración = promedio(Tsecuencial) / promedio(Tparalelo).
- Eficiencia = Aceleración / P * 100, con P = hilos trabajadores efectivos.
- Overhead = P * promedio(Tparalelo) - promedio(Tsecuencial).

DUPLICADOS / MERGE
Dos ZIP/coordenadas distintos solo se agrupan si TODA la serie horaria de la
ventana solicitada es idéntica para las 12 variables de radiación solar.
El CSV final conserva un único ZIP representante de 6 dígitos por grupo; la
trazabilidad de los ZIP agrupados se informa por consola.

COBERTURA GEOGRÁFICA
- Colombia: se priorizan las capas oficiales 4-72/MinTIC y la asociación DANE
  de CodigoPostalAmpliado; la geometría municipal actúa como respaldo.
- Otros países: resolución de mejor esfuerzo con Nominatim/Overpass por área o caja
  delimitadora. La cobertura depende de los datos disponibles en OpenStreetMap.
- Nunca se inventan códigos postales ni coordenadas para aparentar cobertura.

RESILIENCIA DE RED Y API
- Cada lote genera una tarea lógica; una petición meteorológica trae TODA la ventana
  start_date..end_date solicitada. No se fragmenta por hora ni por día.
- --plan-only permite calcular volumen y presupuesto de requests antes de ejecutar.
- --batch-size controla la PARTICIÓN de datos (ZIP por request), NO el número de workers.
- Open-Meteo admite múltiples coordenadas por request; serial y paralelo usan los mismos lotes.
- El pipeline mantiene los workers exigidos, pero limita las requests HTTP
  meteorológicas simultáneas mediante --api-concurrency (2 por defecto).
- Cada request puede contener varias coordenadas mediante --batch-size.
- HTTP 429, HTTP 5xx, timeout y errores transitorios de red se reintentan hasta
  --max-attempts intentos totales (4 por defecto).
- No se añaden retardos artificiales para mejorar resultados.
- Si Open-Meteo responde HTTP 429, se respeta un enfriamiento de rate limit antes de reintentar.
- La espera impuesta por rate limit y todo reintento quedan dentro del tiempo medido.
- Si un ZIP falla definitivamente, la ejecución se marca inválida y no se
  escribe un CSV incompleto.
""",
        formatter_class=ChallengeHelpFormatter,
    )

    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {APP_VERSION}",
    )

    parser.add_argument(
        "--zips",
        nargs="+",
        metavar="ZIP[:CC]",
        help="Conjunto/lista de ZIPs. Ej.: 130001:CO 110111:CO",
    )

    parser.add_argument(
        "--cities",
        nargs="+",
        metavar="CITY",
        help=(
            "Ciudad(es)/municipio(s). Intenta expandir los códigos postales "
            "publicados por el geocodificador."
        ),
    )

    parser.add_argument(
        "--departments",
        nargs="+",
        metavar="DEPARTMENT",
        help=(
            "Departamento(s). En Colombia, auto usa 4-72/MinTIC sin "
            "requerir catálogo. Para otros países use Departamento:CC "
            "y un --catalog con cobertura completa."
        ),
    )

    parser.add_argument(
        "--catalog",
        help=(
            "CSV opcional de ubicaciones. Columnas mínimas: "
            "zip_code,latitude,longitude,city,department"
        ),
    )

    parser.add_argument(
        "--geo-source",
        choices=("auto", "colombia472", "catalog", "openstreet", "openmeteo"),
        default="auto",
        help=(
            "Fuente geográfica. 'colombia472' usa 4-72/MinTIC; ""'openstreet' fuerza Nominatim/Overpass; "
            "'auto' evita llamadas geográficas innecesarias y usa alternativas de respaldo."
        ),
    )

    parser.add_argument(
        "--start",
        help="Inicio de la ventana temporal YYYY-MM-DD",
    )

    parser.add_argument(
        "--end",
        help="Fin de la ventana temporal YYYY-MM-DD",
    )

    parser.add_argument(
        "--start-hour",
        type=parse_hour,
        default=None,
        metavar="HH[:00]",
        help=(
            "Hora inicial diaria opcional. Debe usarse con --end-hour. "
            "Ej.: 08 o 08:00"
        ),
    )

    parser.add_argument(
        "--end-hour",
        type=parse_hour,
        default=None,
        metavar="HH[:00]",
        help=(
            "Hora final diaria opcional e inclusiva. "
            "Ej.: 16 o 16:00"
        ),
    )

    parser.add_argument(
        "--timezone",
        default="auto",
        help=(
            "Zona horaria enviada a Open-Meteo. 'auto' usa la zona local "
            "de cada coordenada."
        ),
    )

    parser.add_argument(
        "--buffer-size",
        type=int,
        default=1,
        help="Capacidad máxima del búfer acotado",
    )

    parser.add_argument(
        "--batch-size",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        metavar="N",
        help=(
            "ZIP/coordenadas por request multi-location. Es granularidad de "
            "partición, NO conteo de workers. "
            f"Predeterminado: {DEFAULT_BATCH_SIZE}."
        ),
    )

    parser.add_argument(
        "--api-concurrency",
        type=int,
        default=DEFAULT_API_CONCURRENCY,
        metavar="N",
        help=(
            "Máximo de solicitudes Open-Meteo simultáneas. "
            f"Predeterminado: {DEFAULT_API_CONCURRENCY}."
        ),
    )

    parser.add_argument(
        "--max-attempts",
        type=int,
        default=DEFAULT_MAX_ATTEMPTS,
        metavar="N",
        help=(
            "Intentos HTTP máximos por lote ante fallos transitorios, "
            f"incluido el intento original. Predeterminado: {DEFAULT_MAX_ATTEMPTS}."
        ),
    )

    parser.add_argument(
        "--repetitions",
        type=int,
        default=1,
        help=(
            "Número de repeticiones por modalidad. "
            "Predeterminado: 1. Use un valor mayor solo para pruebas experimentales."
        ),
    )

    parser.add_argument(
        "--output",
        default="solar_data.csv",
        help=(
            "Único CSV final: trazabilidad del merge + 2 datos geográficos "
            "+ datetime + 12 variables de radiación solar"
        ),
    )

    parser.add_argument(
        "--csv-delimiter",
        choices=("comma", "semicolon"),
        default="comma",
        help=(
            "Delimitador del CSV: comma=',' o semicolon=';'. "
            "Use semicolon si su Excel regional separa columnas con punto y coma."
        ),
    )

    parser.add_argument(
        "--mode",
        choices=("serial", "parallel", "both"),
        default="both",
        help="Modo de ejecución: serial=secuencial, parallel=paralelo, both=ambos",
    )

    parser.add_argument(
        "--timeout",
        type=float,
        default=30.0,
        help="Tiempo máximo de espera HTTP en segundos",
    )

    parser.add_argument(
        "--ssl-mode",
        choices=("auto", "system", "certifi", "insecure"),
        default="auto",
        help=(
            "Validación TLS. 'insecure' existe solo para diagnóstico y "
            "no debe usarse como evidencia final."
        ),
    )

    parser.add_argument(
        "--log-level",
        choices=("DEBUG", "INFO", "WARNING", "ERROR"),
        default="INFO",
        help="Nivel de detalle del registro de ejecución",
    )

    parser.add_argument(
        "--show-catalog",
        action="store_true",
        help="Muestra el catálogo incorporado y termina; no requiere fechas",
    )

    parser.add_argument(
        "--antioquia-dataset",
        action="store_true",
        help=(
            "Genera el dataset histórico de Antioquia por bloques mensuales, "
            "con checkpoint/reanudación y un único CSV final."
        ),
    )

    parser.add_argument(
        "--dataset-start-year",
        type=int,
        default=2015,
        metavar="YYYY",
        help="Año inicial del dataset histórico. Predeterminado: 2015.",
    )

    parser.add_argument(
        "--dataset-end-year",
        type=int,
        default=2025,
        metavar="YYYY",
        help="Año final inclusivo del dataset histórico. Predeterminado: 2025.",
    )

    parser.add_argument(
        "--dataset-output",
        default=None,
        help=(
            "CSV final del dataset. Si se omite: "
            "antioquia_<inicio>_<fin>_NILVER.csv"
        ),
    )

    parser.add_argument(
        "--checkpoint-dir",
        default=".antioquia_solar_checkpoints",
        help=(
            "Directorio interno de checkpoints mensuales comprimidos. "
            "No forma parte de la entrega al docente."
        ),
    )

    parser.add_argument(
        "--force-redownload",
        action="store_true",
        help="Ignora checkpoints existentes y vuelve a descargar cada mes.",
    )

    parser.add_argument(
        "--cleanup-checkpoints",
        action="store_true",
        help=(
            "Borra los checkpoints internos únicamente después de validar "
            "y cerrar correctamente el CSV final."
        ),
    )

    parser.add_argument(
        "--plan-only",
        action="store_true",
        help=(
            "Resuelve la geografía y muestra volumen, workers, tareas y requests "
            "estimadas SIN realizar solicitudes meteorológicas a Open-Meteo."
        ),
    )

    return parser



def expected_rows_per_site(
    start_date: str,
    end_date: str,
    start_hour: int | None,
    end_hour: int | None,
) -> int:
    start_obj = date.fromisoformat(start_date)
    end_obj = date.fromisoformat(end_date)
    days = (end_obj - start_obj).days + 1

    if start_hour is None:
        hours_per_day = 24
    else:
        assert end_hour is not None
        hours_per_day = end_hour - start_hour + 1

    return days * hours_per_day


def raw_rows_from_groups(groups: list[WeatherGroup]) -> int:
    return sum(
        len(group.rows) * len(group.members)
        for group in groups
    )


def timing_stats(values: list[float]) -> dict[str, float]:
    if not values:
        return {
            "mean": 0.0,
            "median": 0.0,
            "stdev": 0.0,
            "min": 0.0,
            "max": 0.0,
        }

    return {
        "mean": statistics.fmean(values),
        "median": statistics.median(values),
        "stdev": statistics.stdev(values) if len(values) > 1 else 0.0,
        "min": min(values),
        "max": max(values),
    }


def aggregate_sync_stats(
    runs: list[ExecutionResult],
) -> dict[str, int]:
    aggregate: dict[str, int] = defaultdict(int)

    for run in runs:
        if not run.sync_stats:
            continue
        for key, value in run.sync_stats.items():
            aggregate[key] += int(value)

    return dict(aggregate)


def system_summary() -> dict[str, str]:
    processor = (
        platform.processor()
        or os.environ.get("PROCESSOR_IDENTIFIER", "")
        or "no disponible"
    )

    return {
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
        "architecture": platform.machine() or "no disponible",
        "processor": processor,
    }


def serial_result_from_call(
    locations: list[Location],
    args: argparse.Namespace,
) -> ExecutionResult:
    (
        groups,
        rows,
        duplicate_inputs,
        elapsed,
        errors,
        request_stats,
    ) = run_serial(
        locations=locations,
        start_date=args.start,
        end_date=args.end,
        timeout=args.timeout,
        ssl_mode=args.ssl_mode,
        timezone=args.timezone,
        start_hour=args.start_hour,
        end_hour=args.end_hour,
        max_attempts=args.max_attempts,
        batch_size=args.batch_size,
    )

    return ExecutionResult(
        groups=groups,
        rows=rows,
        duplicate_inputs=duplicate_inputs,
        elapsed=elapsed,
        errors=errors,
        request_stats=request_stats,
    )

def parallel_result_from_call(
    locations: list[Location],
    args: argparse.Namespace,
    producer_count: int,
    consumer_count: int,
) -> ExecutionResult:
    (
        groups,
        rows,
        duplicate_inputs,
        elapsed,
        errors,
        sync_stats,
        request_stats,
    ) = run_parallel(
        locations=locations,
        start_date=args.start,
        end_date=args.end,
        producer_count=producer_count,
        consumer_count=consumer_count,
        buffer_size=args.buffer_size,
        timeout=args.timeout,
        ssl_mode=args.ssl_mode,
        timezone=args.timezone,
        start_hour=args.start_hour,
        end_hour=args.end_hour,
        api_concurrency=args.api_concurrency,
        max_attempts=args.max_attempts,
        batch_size=args.batch_size,
    )

    return ExecutionResult(
        groups=groups,
        rows=rows,
        duplicate_inputs=duplicate_inputs,
        elapsed=elapsed,
        errors=errors,
        sync_stats=sync_stats,
        request_stats=request_stats,
    )

def same_execution_output(
    left: ExecutionResult,
    right: ExecutionResult,
) -> bool:
    return (
        canonical_final_rows(left.rows)
        == canonical_final_rows(right.rows)
        and canonical_group_summary(left.groups)
        == canonical_group_summary(right.groups)
    )



def execution_site_signatures(
    run: ExecutionResult,
) -> dict[tuple[str, float, float], tuple[Any, ...]]:
    """
    Reconstruye ZIP/coordenada -> firma solar completa a partir
    de los grupos finales. Permite diagnosticar exactamente qué sitio
    cambió entre secuencial y paralelo.
    """
    result: dict[tuple[str, float, float], tuple[Any, ...]] = {}

    for group in run.groups:
        signature = weather_signature(
            SiteSeries(
                location=group.representative,
                rows=group.rows,
            )
        )

        for member in group.members:
            result[
                (
                    member.zip_code,
                    member.latitude,
                    member.longitude,
                )
            ] = signature

    return result


def diagnose_execution_difference(
    serial_run: ExecutionResult,
    parallel_run: ExecutionResult,
    max_items: int = 8,
) -> list[str]:
    serial_map = execution_site_signatures(serial_run)
    parallel_map = execution_site_signatures(parallel_run)

    messages: list[str] = []

    serial_keys = set(serial_map)
    parallel_keys = set(parallel_map)

    only_serial = sorted(serial_keys - parallel_keys)
    only_parallel = sorted(parallel_keys - serial_keys)

    if only_serial:
        messages.append(
            "Sitios presentes solo en secuencial: "
            + ", ".join(key[0] for key in only_serial[:max_items])
        )

    if only_parallel:
        messages.append(
            "Sitios presentes solo en paralelo: "
            + ", ".join(key[0] for key in only_parallel[:max_items])
        )

    changed = sorted(
        key
        for key in serial_keys & parallel_keys
        if serial_map[key] != parallel_map[key]
    )

    for key in changed[:max_items]:
        serial_sig = serial_map[key]
        parallel_sig = parallel_map[key]

        first_difference = None

        for index, (left, right) in enumerate(
            zip(serial_sig, parallel_sig)
        ):
            if left != right:
                first_difference = (index, left, right)
                break

        zip_code = key[0]

        if first_difference is None:
            messages.append(
                f"ZIP {zip_code}: longitud de serie distinta "
                f"(sec={len(serial_sig)}, par={len(parallel_sig)})."
            )
        else:
            index, left, right = first_difference
            messages.append(
                f"ZIP {zip_code}: primera diferencia en registro "
                f"{index}: sec={left!r} | par={right!r}"
            )

    if not messages:
        messages.append(
            "Las series por sitio coinciden; la diferencia está en la "
            "metainformación/agrupación final."
        )

    return messages



def repetitions_consistent(
    runs: list[ExecutionResult],
) -> bool:
    if not runs:
        return False

    baseline_rows = canonical_final_rows(runs[0].rows)
    baseline_groups = canonical_group_summary(runs[0].groups)

    return all(
        canonical_final_rows(run.rows) == baseline_rows
        and canonical_group_summary(run.groups) == baseline_groups
        for run in runs[1:]
    )


def aggregate_request_stats(
    runs: list[ExecutionResult],
) -> dict[str, float | int]:
    aggregate: dict[str, float] = defaultdict(float)

    for run in runs:
        if not run.request_stats:
            continue
        for key, value in run.request_stats.items():
            aggregate[key] += float(value)

    result: dict[str, float | int] = {}
    for key, value in aggregate.items():
        if key == "segundos_espera_rate_limit":
            result[key] = value
        else:
            result[key] = int(value)
    return result


def print_request_summary(
    serial_runs: list[ExecutionResult],
    parallel_runs: list[ExecutionResult],
    api_concurrency: int,
    max_attempts: int,
) -> None:
    print_separator("RESILIENCIA Y CONTROL DE API")
    print(f"Concurrencia HTTP máxima configurada: {api_concurrency}")
    print(f"Intentos máximos por solicitud lógica: {max_attempts}")
    print(
        "Política: se reintentan 429, HTTP 5xx, timeout y errores "
        "transitorios de red; ante 429 se respeta el enfriamiento del servicio, "
        "sin añadir retardos artificiales de rendimiento."
    )

    for label, runs in (("Secuencial", serial_runs), ("Paralelo", parallel_runs)):
        if not runs:
            continue
        stats = aggregate_request_stats(runs)
        print(f"{label}:")
        print(
            "  Solicitudes lógicas / intentos HTTP: "
            f"{stats.get('solicitudes_logicas', 0)} / "
            f"{stats.get('intentos_http', 0)}"
        )
        print(
            "  Reintentos / recuperadas / fallos definitivos: "
            f"{stats.get('reintentos', 0)} / "
            f"{stats.get('solicitudes_recuperadas', 0)} / "
            f"{stats.get('fallos_definitivos', 0)}"
        )
        print(
            "  Eventos 429 / 5xx / timeout / red: "
            f"{stats.get('eventos_http_429', 0)} / "
            f"{stats.get('eventos_http_5xx', 0)} / "
            f"{stats.get('eventos_timeout', 0)} / "
            f"{stats.get('eventos_red', 0)}"
        )
        print(
            "  Esperas rate-limit / tiempo acumulado: "
            f"{stats.get('esperas_rate_limit', 0)} / "
            f"{float(stats.get('segundos_espera_rate_limit', 0.0)):.1f} s"
        )


def print_sync_summary(
    parallel_runs: list[ExecutionResult],
    repetitions: int,
) -> None:
    stats = aggregate_sync_stats(parallel_runs)

    print_separator("RESUMEN DE SINCRONIZACIÓN")

    if not stats:
        print("No hay métricas de sincronización disponibles.")
        return

    print(
        f"Repeticiones paralelas:          {repetitions}"
    )
    print(
        "Esperas productor por buffer lleno: "
        f"{stats.get('esperas_productores_buffer_lleno', 0)}"
    )
    print(
        "Esperas consumidor por buffer vacío: "
        f"{stats.get('esperas_consumidores_buffer_vacio', 0)}"
    )
    print(
        "Elementos insertados / extraídos: "
        f"{stats.get('elementos_insertados', 0)} / "
        f"{stats.get('elementos_extraidos', 0)}"
    )
    print(
        "Señales a consumidores / productores: "
        f"{stats.get('senales_a_consumidores', 0)} / "
        f"{stats.get('senales_a_productores', 0)}"
    )
    print(
        "notify_all para cierre:          "
        f"{stats.get('notify_all_cierre', 0)}"
    )
    print(
        "Productores finalizados:         "
        f"{stats.get('productores_finalizados', 0)}"
    )
    print(
        "Consumidores cerrados:           "
        f"{stats.get('consumidores_cerrados', 0)}"
    )
    print(
        "Hilos esperados con join():      "
        f"{stats.get('hilos_join_realizados', 0)}"
    )
    print(
        "Tareas/lotes lógicos totales:    "
        f"{stats.get('tareas_logicas_totales', 0)}"
    )
    print(
        "Lotes asignados dinámicamente:   "
        f"{stats.get('tareas_asignadas', 0)}"
    )
    print(
        "Tareas/lotes pendientes cierre:  "
        f"{stats.get('tareas_pendientes', 0)}"
    )
    print(
        "Sitios cubiertos por tareas:     "
        f"{stats.get('sitios_en_tareas', 0)}"
    )
    print(
        "Tamaño de lote configurado:      "
        f"{max((run.sync_stats or {}).get('tamano_lote_configurado', 0) for run in parallel_runs)}"
    )
    print(
        "Ocupación máxima observada:      "
        f"{max((run.sync_stats or {}).get('ocupacion_maxima_buffer', 0) for run in parallel_runs)}"
    )
    print(
        "Concurrencia API máxima observada: "
        f"{max((run.sync_stats or {}).get('concurrencia_api_max_observada', 0) for run in parallel_runs)}"
    )
    print(
        "Límite configurado de concurrencia API: "
        f"{max((run.sync_stats or {}).get('limite_concurrencia_api', 0) for run in parallel_runs)}"
    )
    print(
        "Esperas por límite de concurrencia API: "
        f"{sum((run.sync_stats or {}).get('esperas_limite_api', 0) for run in parallel_runs)}"
    )


def print_data_integrity(
    groups: list[WeatherGroup],
    rows: list[dict[str, Any]],
    locations: list[Location],
    args: argparse.Namespace,
) -> None:
    per_site = expected_rows_per_site(
        args.start,
        args.end,
        args.start_hour,
        args.end_hour,
    )
    expected_raw = len(locations) * per_site
    acquired_raw = raw_rows_from_groups(groups)
    final_rows = len(rows)
    traced_sites = sum(len(group.members) for group in groups)

    removed = max(0, acquired_raw - final_rows)
    reduction = (
        100.0 * removed / acquired_raw
        if acquired_raw
        else 0.0
    )

    print_separator("INTEGRIDAD Y TRAZABILIDAD DE DATOS")
    print(f"Filas esperadas por sitio:       {per_site}")
    print(f"Filas crudas esperadas:          {expected_raw}")
    print(f"Filas crudas adquiridas:         {acquired_raw}")
    print(
        "Cobertura de adquisición:         "
        + ("COMPLETA" if acquired_raw == expected_raw else "INCOMPLETA")
    )
    print(f"Sitios trazados en grupos:       {traced_sites}/{len(locations)}")
    print(f"Series solares únicas:          {len(groups)}")
    print(f"Filas finales sin repetición:    {final_rows}")
    print(f"Filas evitadas por agrupación:   {removed}")
    print(f"Reducción por deduplicación:     {reduction:.2f}%")


def print_engineering_conclusions(
    valid_experiment: bool,
    serial_runs: list[ExecutionResult],
    parallel_runs: list[ExecutionResult],
    total_worker_threads: int,
    logical_cores: int,
    locations: list[Location],
) -> None:
    print_separator("ANÁLISIS Y CONCLUSIONES")

    if not valid_experiment:
        print(
            "- El experimento no se considera válido; no se obtienen "
            "conclusiones de rendimiento."
        )
        return

    print(
        "- Todas las ejecuciones válidas conservaron la misma carga de "
        "trabajo y resultados."
    )
    print(
        "- wait() se ejecuta dentro de while: al despertar se vuelve a "
        "comprobar el estado compartido antes de continuar."
    )
    print(
        "- notify()/notify_all() permiten reactivar hilos bloqueados, pero "
        "no sustituyen la revalidación de la condición."
    )
    print(
        "- join() se aplica a todos los productores y consumidores antes "
        "de validar tiempos o escribir resultados."
    )
    print(
        "- La resolución geográfica y la escritura del CSV quedan fuera "
        "del cronometraje para comparar el mismo trabajo meteorológico."
    )
    print(
        f"- Se procesaron {len(locations)} sitios utilizando "
        f"{total_worker_threads} hilos trabajadores, correspondientes a los "
        f"{logical_cores} núcleos lógicos detectados."
    )

    if serial_runs and parallel_runs:
        serial_mean = statistics.fmean(run.elapsed for run in serial_runs)
        parallel_mean = statistics.fmean(run.elapsed for run in parallel_runs)
        speedup = serial_mean / parallel_mean if parallel_mean > 0 else 0.0

        if speedup > 1.0:
            print(
                f"- La versión paralela redujo el tiempo promedio; "
                f"aceleración global {speedup:.4f}x."
            )
        else:
            print(
                "- No hubo aceleración promedio. En una canalización con "
                "E/S, latencia de red y sincronización pueden dominar."
            )

        print(
            "- La eficiencia y el sobrecosto (overhead) se reportan con P igual al total "
            "de hilos trabajadores; son métricas experimentales de esta "
            "configuración, no una medición directa del uso físico de CPU."
        )
        print(
            "- La concurrencia hacia Open-Meteo se limita de forma "
            "independiente al número de hilos del pipeline para respetar "
            "la capacidad del recurso externo."
        )
        print(
            "- Los fallos transitorios se reintentan de forma acotada y "
            "visible; si persisten, la ejecución se invalida y no se "
            "escribe un CSV incompleto."
        )



# ---------------------------------------------------------------------------
# FUNCIÓN PRINCIPAL
# ---------------------------------------------------------------------------


def interactive_argv() -> list[str]:
    """
    Modo guiado para la revisión del docente.

    Si se ejecuta simplemente:
        python chal.py

    el programa pregunta los datos mínimos y luego ejecuta el mismo pipeline
    que se usaría desde CLI. No introduce un camino alterno de cálculo:
    únicamente construye los mismos argumentos que argparse recibe.
    """
    print_separator("CHALLENGE 7 - MODO INTERACTIVO")
    print("Seleccione el alcance geográfico:")
    print("  1) ZIP / set de ZIPs")
    print("  2) Ciudad / municipio")
    print("  3) Departamento (Colombia: resolución automática 4-72)")
    print()

    while True:
        scope = input("Opción [1-3]: ").strip()
        if scope in {"1", "2", "3"}:
            break
        print("[ERROR] Ingrese 1, 2 o 3.")

    args: list[str] = []

    if scope == "1":
        raw = input(
            "ZIP(s), separados por espacios "
            "(ej. 130001:CO 110111:CO): "
        ).strip()
        values = raw.split()
        if not values:
            raise ChallengeError("Debe indicar al menos un ZIP.")
        args.extend(["--zips", *values])

    elif scope == "2":
        raw = input(
            "Ciudad(es)/municipio(s), separadas por coma "
            "(ej. Cartagena, Medellin, Lima:PE): "
        ).strip()
        values = [item.strip() for item in raw.split(",") if item.strip()]
        if not values:
            raise ChallengeError("Debe indicar al menos una ciudad.")
        args.extend(["--cities", *values])

    else:
        raw = input(
            "Departamento(s), separados por coma "
            "(ej. Bolivar, Antioquia): "
        ).strip()
        values = [item.strip() for item in raw.split(",") if item.strip()]
        if not values:
            raise ChallengeError("Debe indicar al menos un departamento.")

        args.extend(["--departments", *values])

    start = input("Fecha inicial [YYYY-MM-DD]: ").strip()
    end = input("Fecha final   [YYYY-MM-DD]: ").strip()

    if not start or not end:
        raise ChallengeError(
            "La ventana temporal requiere fecha inicial y final."
        )

    args.extend(["--start", start, "--end", end])

    print()
    hour_start = input(
        "Hora inicial opcional [HH o HH:00, Enter = todo el día]: "
    ).strip()

    if hour_start:
        hour_end = input(
            "Hora final inclusiva [HH o HH:00]: "
        ).strip()

        if not hour_end:
            raise ChallengeError(
                "Si indica hora inicial también debe indicar hora final."
            )

        args.extend(
            [
                "--start-hour", hour_start,
                "--end-hour", hour_end,
            ]
        )

    repetitions_raw = input(
        "Repeticiones [Enter=1; use un valor mayor solo para pruebas]: "
    ).strip()

    if not repetitions_raw:
        repetitions = 1
    else:
        try:
            repetitions = int(repetitions_raw)
        except ValueError as exc:
            raise ChallengeError(
                "El número de repeticiones debe ser un entero positivo."
            ) from exc

        if repetitions <= 0:
            raise ChallengeError(
                "El número de repeticiones debe ser un entero positivo."
            )

    # Valores predeterminados deliberados:
    # - workers fijos: productores + consumidores = os.cpu_count();
    # - búfer 1: evidencia clara de ESPERA/SEÑAL;
    # - una repetición por defecto: suficiente para la ejecución normal del reto;
    # - modo ambos: secuencial + paralelo para validar igualdad y aceleración;
    # - fuente geográfica automática: evita depender innecesariamente de un único servicio.
    args.extend(
        [
            "--buffer-size", "1",
            "--batch-size", str(DEFAULT_BATCH_SIZE),
            "--api-concurrency", str(DEFAULT_API_CONCURRENCY),
            "--max-attempts", str(DEFAULT_MAX_ATTEMPTS),
            "--repetitions", str(repetitions),
            "--mode", "both",
            "--geo-source", "auto",
            "--ssl-mode", "auto",
            "--output", "solar_data.csv",
        ]
    )

    print()
    print("Configuración automática:")
    print("  Workers:      todos los núcleos lógicos detectados (conteo fijo)")
    print("  Búfer:                 1")
    print(f"  ZIP por lote/request:  {DEFAULT_BATCH_SIZE}")
    print(f"  Concurrencia API máx.: {DEFAULT_API_CONCURRENCY}")
    print(f"  Intentos HTTP máx.:    {DEFAULT_MAX_ATTEMPTS}")
    print(f"  Repeticiones:          {repetitions}")
    print("  Modo:         ambos (secuencial + paralelo)")
    print("  Fuente geo.:  automática")
    print("  Zona horaria: automática (local por coordenada)")
    print("  Salida:       solar_data.csv")
    print()

    return args



# ---------------------------------------------------------------------------
# DATASET HISTÓRICO ANTIOQUIA 2015-2025
# ---------------------------------------------------------------------------

EXPECTED_ANTIOQUIA_ZIPS = 384
DATASET_INTERNAL_FIELDS = FINAL_CSV_FIELDS


def _month_bounds(year: int, month: int) -> tuple[str, str, int]:
    last_day = calendar.monthrange(year, month)[1]
    start = f"{year:04d}-{month:02d}-01"
    end = f"{year:04d}-{month:02d}-{last_day:02d}"
    return start, end, last_day * 24


def _dataset_month_paths(
    checkpoint_dir: Path,
    year: int,
    month: int,
) -> tuple[Path, Path]:
    stem = f"antioquia_{year:04d}_{month:02d}"
    return (
        checkpoint_dir / f"{stem}.csv.gz",
        checkpoint_dir / f"{stem}.json",
    )


def _canonical_solar_bytes(row: dict[str, Any]) -> bytes:
    values = [row["datetime"]] + [row[name] for name in HOURLY_VARIABLES]
    return (
        json.dumps(
            values,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _normalize_dataset_item_allow_missing(
    item: PipelineItem,
) -> tuple[SiteSeries, dict[str, int], list[tuple[str, str]]]:
    """
    Normalización exclusiva del dataset histórico.

    Para una ventana histórica extensa, Open-Meteo puede devolver ``null`` en
    una variable solar aislada. El dato faltante se PRESERVA como faltante; no
    se reemplaza por 0, no se interpola y no se elimina la hora. De este modo
    el CSV mantiene la serie temporal completa y no inventa mediciones.

    Retorna la serie, conteos de faltantes por variable y ejemplos
    (variable, datetime) para auditoría.
    """
    hourly = item.payload.get("hourly")
    if not isinstance(hourly, dict):
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: respuesta sin 'hourly'."
        )

    required = ("time",) + HOURLY_VARIABLES
    missing_fields = [name for name in required if name not in hourly]
    if missing_fields:
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: faltan columnas {missing_fields}."
        )

    arrays = [hourly[name] for name in required]
    if not all(isinstance(values, list) for values in arrays):
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: hourly no contiene listas válidas."
        )

    lengths = {len(values) for values in arrays}
    if len(lengths) != 1:
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: longitudes horarias inconsistentes."
        )

    rows: list[dict[str, Any]] = []
    missing_counts = {name: 0 for name in HOURLY_VARIABLES}
    examples: list[tuple[str, str]] = []

    for i, timestamp in enumerate(hourly["time"]):
        dt = str(timestamp)
        row: dict[str, Any] = {"datetime": timestamp}
        for variable in HOURLY_VARIABLES:
            value = hourly[variable][i]
            if value is None:
                missing_counts[variable] += 1
                if len(examples) < 8:
                    examples.append((variable, dt))
            row[variable] = value
        rows.append(row)

    if not rows:
        raise DataValidationError(
            f"ZIP {item.location.zip_code}: la ventana no produjo registros."
        )

    return (
        SiteSeries(location=item.location, rows=tuple(rows)),
        missing_counts,
        examples,
    )


def _atomic_json(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(
        json.dumps(data, ensure_ascii=False, sort_keys=True, indent=2),
        encoding="utf-8",
    )
    tmp.replace(path)


def _load_valid_month_checkpoint(
    checkpoint_dir: Path,
    year: int,
    month: int,
    expected_zips: list[str],
) -> dict[str, Any] | None:
    data_path, manifest_path = _dataset_month_paths(checkpoint_dir, year, month)
    if not data_path.exists() or not manifest_path.exists():
        return None

    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    if not isinstance(manifest, dict):
        return None

    _, _, hours = _month_bounds(year, month)
    expected_rows = len(expected_zips) * hours

    if manifest.get("year") != year or manifest.get("month") != month:
        return None
    if manifest.get("zip_count") != len(expected_zips):
        return None
    if manifest.get("row_count") != expected_rows:
        return None
    if manifest.get("variables") != list(HOURLY_VARIABLES):
        return None

    hashes = manifest.get("zip_sha256")
    counts = manifest.get("zip_rows")
    if not isinstance(hashes, dict) or not isinstance(counts, dict):
        return None

    if set(hashes) != set(expected_zips) or set(counts) != set(expected_zips):
        return None
    if any(int(counts[z]) != hours for z in expected_zips):
        return None

    if data_path.stat().st_size <= 0:
        return None

    return manifest


def _download_dataset_month(
    locations: list[Location],
    year: int,
    month: int,
    checkpoint_dir: Path,
    timeout: float,
    ssl_mode: str,
    timezone: str,
    batch_size: int,
    api_concurrency: int,
    max_attempts: int,
) -> dict[str, Any]:
    start_date, end_date, expected_hours = _month_bounds(year, month)
    data_path, manifest_path = _dataset_month_paths(checkpoint_dir, year, month)
    tmp_data = data_path.with_suffix(data_path.suffix + ".tmp")

    for stale in (tmp_data, manifest_path.with_suffix(manifest_path.suffix + ".tmp")):
        try:
            stale.unlink()
        except FileNotFoundError:
            pass

    batches = make_location_batches(locations, batch_size)
    request_stats = RequestStatsCollector()
    rate_limit = RateLimitCoordinator()
    series_by_zip: dict[str, SiteSeries] = {}
    missing_by_variable: dict[str, int] = {name: 0 for name in HOURLY_VARIABLES}
    missing_examples: list[dict[str, str]] = []

    print(
        f"  {year:04d}-{month:02d}: descargando {len(locations)} ZIP "
        f"({start_date} -> {end_date}, {len(batches)} lotes)..."
    )

    def fetch_one(batch: LocationBatch) -> list[PipelineItem]:
        return fetch_solar_batch_resilient(
            batch=batch,
            start_date=start_date,
            end_date=end_date,
            timeout=timeout,
            ssl_mode=ssl_mode,
            timezone=timezone,
            max_attempts=max_attempts,
            request_stats=request_stats,
            requester_name=f"DATASET-{year:04d}-{month:02d}",
            rate_limit=rate_limit,
        )

    workers = max(1, min(api_concurrency, len(batches)))
    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_to_batch = {
                executor.submit(fetch_one, batch): batch
                for batch in batches
            }
            for future in as_completed(future_to_batch):
                batch = future_to_batch[future]
                try:
                    items = future.result()
                except Exception as exc:
                    raise ChallengeError(
                        f"{year:04d}-{month:02d} falló en {batch.label}: {exc}"
                    ) from exc

                for item in items:
                    series, item_missing, item_examples = (
                        _normalize_dataset_item_allow_missing(item)
                    )
                    zip_code = str(series.location.zip_code)
                    for variable, count in item_missing.items():
                        missing_by_variable[variable] += int(count)
                    for variable, dt in item_examples:
                        if len(missing_examples) < 20:
                            missing_examples.append(
                                {
                                    "zipcode": zip_code,
                                    "datetime": dt,
                                    "variable": variable,
                                }
                            )
                    if zip_code in series_by_zip:
                        raise DataValidationError(
                            f"{year:04d}-{month:02d}: ZIP duplicado {zip_code}."
                        )
                    if len(series.rows) != expected_hours:
                        raise DataValidationError(
                            f"{year:04d}-{month:02d}/ZIP {zip_code}: "
                            f"{len(series.rows)} horas, se esperaban {expected_hours}."
                        )
                    expected_first = f"{start_date}T00:00"
                    expected_last = f"{end_date}T23:00"
                    actual_first = str(series.rows[0]["datetime"])
                    actual_last = str(series.rows[-1]["datetime"])
                    if actual_first != expected_first or actual_last != expected_last:
                        raise DataValidationError(
                            f"{year:04d}-{month:02d}/ZIP {zip_code}: rango horario "
                            f"{actual_first} -> {actual_last}; se esperaba "
                            f"{expected_first} -> {expected_last}."
                        )
                    series_by_zip[zip_code] = series

        expected_zip_set = {loc.zip_code for loc in locations}
        if set(series_by_zip) != expected_zip_set:
            missing = sorted(expected_zip_set - set(series_by_zip))
            extra = sorted(set(series_by_zip) - expected_zip_set)
            raise DataValidationError(
                f"{year:04d}-{month:02d}: cobertura ZIP incompleta. "
                f"Faltan={missing[:10]} extras={extra[:10]}"
            )

        zip_hashes: dict[str, str] = {}
        zip_rows: dict[str, int] = {}
        row_count = 0

        with gzip.open(
            tmp_data,
            "wt",
            encoding="utf-8",
            newline="",
            compresslevel=6,
        ) as file:
            writer = csv.DictWriter(
                file,
                fieldnames=DATASET_INTERNAL_FIELDS,
                delimiter=";",
                quoting=csv.QUOTE_MINIMAL,
            )
            writer.writeheader()

            for zip_code in sorted(series_by_zip):
                series = series_by_zip[zip_code]
                hasher = hashlib.sha256()
                previous_datetime = ""

                for weather in series.rows:
                    dt = str(weather["datetime"])
                    if previous_datetime and dt <= previous_datetime:
                        raise DataValidationError(
                            f"{year:04d}-{month:02d}/ZIP {zip_code}: "
                            "datetime no es estrictamente creciente."
                        )
                    previous_datetime = dt
                    hasher.update(_canonical_solar_bytes(weather))

                    row: dict[str, Any] = {
                        "zipcode": zip_code,
                        "datetime": dt,
                        "representative_latitude": series.location.latitude,
                        "representative_longitude": series.location.longitude,
                    }
                    for variable in HOURLY_VARIABLES:
                        row[variable] = weather[variable]
                    writer.writerow(row)
                    row_count += 1

                zip_hashes[zip_code] = hasher.hexdigest()
                zip_rows[zip_code] = len(series.rows)

        expected_rows = len(locations) * expected_hours
        if row_count != expected_rows:
            raise DataValidationError(
                f"{year:04d}-{month:02d}: {row_count} filas; "
                f"se esperaban {expected_rows}."
            )

        tmp_data.replace(data_path)
        manifest = {
            "version": APP_VERSION,
            "dataset": "Antioquia solar",
            "year": year,
            "month": month,
            "start_date": start_date,
            "end_date": end_date,
            "zip_count": len(locations),
            "hours_per_zip": expected_hours,
            "row_count": row_count,
            "variables": list(HOURLY_VARIABLES),
            "zip_sha256": zip_hashes,
            "zip_rows": zip_rows,
            "request_stats": request_stats.snapshot(),
            "missing_solar_total": sum(missing_by_variable.values()),
            "missing_solar_by_variable": missing_by_variable,
            "missing_solar_examples": missing_examples,
        }
        _atomic_json(manifest_path, manifest)

        missing_total = sum(missing_by_variable.values())
        print(
            f"  {year:04d}-{month:02d}: OK | {row_count:,} filas | "
            f"{data_path.name} | faltantes solares preservados={missing_total:,}"
        )
        if missing_total:
            print(
                "    Nota: los null de Open-Meteo se conservaron como celdas vacías; "
                "NO se imputaron, NO se reemplazaron por 0 y NO se eliminó la hora."
            )
        return manifest

    except Exception:
        try:
            tmp_data.unlink()
        except FileNotFoundError:
            pass
        raise


def _dataset_signature_groups(
    manifests: list[dict[str, Any]],
    zip_codes: list[str],
) -> list[list[str]]:
    signatures: dict[str, tuple[str, ...]] = {}

    for zip_code in zip_codes:
        parts: list[str] = []
        for manifest in manifests:
            hashes = manifest.get("zip_sha256")
            if not isinstance(hashes, dict) or zip_code not in hashes:
                raise DataValidationError(
                    f"Checkpoint sin firma para ZIP {zip_code}."
                )
            parts.append(str(hashes[zip_code]))
        signatures[zip_code] = tuple(parts)

    buckets: dict[tuple[str, ...], list[str]] = defaultdict(list)
    for zip_code, signature in signatures.items():
        buckets[signature].append(zip_code)

    groups = [sorted(values) for values in buckets.values()]
    groups.sort(key=lambda values: values[0])
    return groups


def _write_dataset_final_csv(
    checkpoint_dir: Path,
    manifests: list[dict[str, Any]],
    groups: list[list[str]],
    output_path: Path,
) -> tuple[int, dict[str, int], dict[str, int]]:
    representatives = {group[0] for group in groups}
    expected_hours = sum(int(m["hours_per_zip"]) for m in manifests)
    expected_total_rows = expected_hours * len(representatives)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_output = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        tmp_output.unlink()
    except FileNotFoundError:
        pass

    per_zip_rows: dict[str, int] = {z: 0 for z in representatives}
    first_datetime: dict[str, str] = {z: "" for z in representatives}
    last_datetime: dict[str, str] = {z: "" for z in representatives}
    total_rows = 0
    missing_solar_by_variable: dict[str, int] = {
        name: 0 for name in HOURLY_VARIABLES
    }

    with tmp_output.open("w", encoding="utf-8-sig", newline="") as dst:
        writer = csv.DictWriter(
            dst,
            fieldnames=FINAL_CSV_FIELDS,
            delimiter=";",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()

        for manifest in manifests:
            year = int(manifest["year"])
            month = int(manifest["month"])
            data_path, _ = _dataset_month_paths(checkpoint_dir, year, month)

            with gzip.open(data_path, "rt", encoding="utf-8", newline="") as src:
                reader = csv.DictReader(src, delimiter=";")
                if tuple(reader.fieldnames or ()) != FINAL_CSV_FIELDS:
                    raise DataValidationError(
                        f"Checkpoint {data_path.name}: encabezado inesperado."
                    )

                for row in reader:
                    zip_code = str(row.get("zipcode") or "").strip()
                    if zip_code not in representatives:
                        continue
                    if not re.fullmatch(r"05\d{4}", zip_code):
                        raise DataValidationError(
                            f"ZIP Antioquia inválido en checkpoint: {zip_code!r}."
                        )

                    dt = str(row.get("datetime") or "")
                    previous = last_datetime[zip_code]
                    if previous and dt <= previous:
                        raise DataValidationError(
                            f"ZIP {zip_code}: datetime duplicado/desordenado: {dt}."
                        )
                    if not first_datetime[zip_code]:
                        first_datetime[zip_code] = dt
                    last_datetime[zip_code] = dt

                    for field in (
                        "zipcode",
                        "datetime",
                        "representative_latitude",
                        "representative_longitude",
                    ):
                        if row.get(field) in (None, ""):
                            raise DataValidationError(
                                f"Valor obligatorio vacío en {field}, "
                                f"ZIP {zip_code}, {dt}."
                            )

                    for variable in HOURLY_VARIABLES:
                        if row.get(variable) in (None, ""):
                            missing_solar_by_variable[variable] += 1

                    writer.writerow(row)
                    per_zip_rows[zip_code] += 1
                    total_rows += 1

    if total_rows != expected_total_rows:
        raise DataValidationError(
            f"CSV final: {total_rows} filas, se esperaban {expected_total_rows}."
        )

    bad_counts = {
        z: count
        for z, count in per_zip_rows.items()
        if count != expected_hours
    }
    if bad_counts:
        raise DataValidationError(
            "CSV final con ZIP incompletos: "
            + ", ".join(f"{z}={c}" for z, c in list(bad_counts.items())[:10])
        )

    first_manifest = manifests[0]
    last_manifest = manifests[-1]
    expected_first = str(first_manifest["start_date"]) + "T00:00"
    expected_last = str(last_manifest["end_date"]) + "T23:00"
    bad_ranges = [
        z for z in representatives
        if first_datetime[z] != expected_first or last_datetime[z] != expected_last
    ]
    if bad_ranges:
        raise DataValidationError(
            "CSV final con rango temporal incompleto en ZIP: "
            + ", ".join(sorted(bad_ranges)[:10])
        )

    tmp_output.replace(output_path)
    return total_rows, per_zip_rows, missing_solar_by_variable


def run_antioquia_dataset(args: argparse.Namespace) -> int:
    start_year = int(args.dataset_start_year)
    end_year = int(args.dataset_end_year)

    if start_year < 1940:
        raise ChallengeError("El año inicial no puede ser anterior a 1940.")
    if start_year > end_year:
        raise ChallengeError("dataset-start-year no puede ser mayor que dataset-end-year.")
    if end_year >= date.today().year:
        raise ChallengeError(
            "Para un dataset anual completo, dataset-end-year debe ser anterior "
            "al año actual. Para la entrega solicitada use 2015..2025."
        )
    if args.batch_size <= 0 or args.api_concurrency <= 0 or args.max_attempts <= 0:
        raise ChallengeError("batch-size, api-concurrency y max-attempts deben ser > 0.")

    checkpoint_dir = Path(args.checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)

    print_separator("DATASET HISTÓRICO ANTIOQUIA")
    print(f"Versión:                       {APP_VERSION}")
    print("Departamento:                  Antioquia")
    print(f"Periodo:                       {start_year}-01-01 -> {end_year}-12-31")
    print(f"Años completos:                {end_year - start_year + 1}")
    print("Fuente geográfica:             4-72 / MinTIC")
    print("Variables solares:             12")
    print("Salida del docente:            UN único CSV final")
    print("Checkpoints:                    mensuales, comprimidos, internos")

    locations = discover_colombia_department_postcodes_472(
        "Antioquia",
        args.timeout,
        args.ssl_mode,
    )
    locations, _ = unique_locations(locations)
    zip_codes = sorted(loc.zip_code for loc in locations)

    if len(locations) != EXPECTED_ANTIOQUIA_ZIPS:
        raise DataValidationError(
            f"Antioquia: se esperaban {EXPECTED_ANTIOQUIA_ZIPS} códigos postales "
            f"según el alcance validado con el docente, pero 4-72 devolvió {len(locations)}. "
            "Se detiene para no generar un CSV con cobertura distinta."
        )

    invalid_zips = [z for z in zip_codes if not re.fullmatch(r"05\d{4}", z)]
    if invalid_zips:
        raise DataValidationError(
            "Se encontraron ZIP de Antioquia fuera del formato 05xxxx: "
            + ", ".join(invalid_zips[:10])
        )

    total_days = sum(
        (date(year, 12, 31) - date(year, 1, 1)).days + 1
        for year in range(start_year, end_year + 1)
    )
    total_hours_per_zip = total_days * 24
    raw_rows = total_hours_per_zip * len(locations)
    month_count = (end_year - start_year + 1) * 12
    batches_per_month = (len(locations) + args.batch_size - 1) // args.batch_size
    logical_requests = month_count * batches_per_month

    print(f"ZIP validados:                  {len(locations)} / {EXPECTED_ANTIOQUIA_ZIPS}")
    print(f"Formato ZIP:                    05xxxx (6 dígitos, string)")
    print(f"Días del periodo:               {total_days:,}")
    print(f"Horas esperadas por ZIP:        {total_hours_per_zip:,}")
    print(f"Filas brutas antes de agrupar:  {raw_rows:,}")
    print(f"Meses a procesar:               {month_count}")
    print(f"Lotes por mes:                  {batches_per_month}")
    print(f"Requests lógicas estimadas:     {logical_requests:,}")
    print(f"Concurrencia API:               {args.api_concurrency}")
    print(f"Checkpoint dir:                 {checkpoint_dir.resolve()}")

    if args.plan_only:
        print("\nPLAN-ONLY: no se descargaron datos solares.")
        return 0

    manifests: list[dict[str, Any]] = []

    for year in range(start_year, end_year + 1):
        print_separator(f"AÑO {year}")
        for month in range(1, 13):
            manifest = None
            if not args.force_redownload:
                manifest = _load_valid_month_checkpoint(
                    checkpoint_dir,
                    year,
                    month,
                    zip_codes,
                )

            if manifest is not None:
                print(
                    f"  {year:04d}-{month:02d}: checkpoint válido -> REUTILIZADO "
                    f"({int(manifest['row_count']):,} filas)"
                )
            else:
                manifest = _download_dataset_month(
                    locations=locations,
                    year=year,
                    month=month,
                    checkpoint_dir=checkpoint_dir,
                    timeout=args.timeout,
                    ssl_mode=args.ssl_mode,
                    timezone=args.timezone,
                    batch_size=args.batch_size,
                    api_concurrency=args.api_concurrency,
                    max_attempts=args.max_attempts,
                )
            manifests.append(manifest)

        year_rows = sum(
            int(m["row_count"])
            for m in manifests
            if int(m["year"]) == year
        )
        print(f"Año {year}: VALIDADO | {year_rows:,} filas brutas")

    print_separator("AGRUPACIÓN GLOBAL DEL PERIODO COMPLETO")
    groups = _dataset_signature_groups(manifests, zip_codes)
    representatives = [group[0] for group in groups]
    redundant = len(zip_codes) - len(groups)
    repeated_groups = [group for group in groups if len(group) > 1]

    print(f"ZIP originales:                  {len(zip_codes)}")
    print(f"Series únicas {start_year}-{end_year}:         {len(groups)}")
    print(f"ZIP redundantes agrupados:       {redundant}")
    print(f"Grupos con 2+ ZIP:               {len(repeated_groups)}")
    print(
        "Regla:                           un ZIP se agrupa solo si sus "
        f"{len(manifests)} firmas mensuales SHA-256 coinciden en todo el periodo."
    )

    for group in repeated_groups:
        print(f"  representante {group[0]} <- " + ", ".join(group[1:]))

    if args.dataset_output:
        output_path = Path(args.dataset_output)
    else:
        output_path = Path(
            f"antioquia_{start_year}_{end_year}_NILVER.csv"
        )

    print_separator("CONSTRUCCIÓN DEL ÚNICO CSV FINAL")
    print(f"Representantes a exportar:       {len(representatives)}")
    print(f"Archivo:                         {output_path.resolve()}")

    total_rows, per_zip_rows, final_missing_by_variable = (
        _write_dataset_final_csv(
            checkpoint_dir,
            manifests,
            groups,
            output_path,
        )
    )
    final_missing_total = sum(final_missing_by_variable.values())

    expected_first = f"{start_year:04d}-01-01T00:00"
    expected_last = f"{end_year:04d}-12-31T23:00"
    sample_zip = representatives[0]

    print_separator("VALIDACIÓN FINAL")
    print(f"Periodo esperado:                {expected_first} -> {expected_last}")
    print(f"Años completos:                  {end_year - start_year + 1}/{end_year - start_year + 1}")
    print(f"ZIP originales considerados:     {len(zip_codes)}/{EXPECTED_ANTIOQUIA_ZIPS}")
    print(f"ZIP representativos finales:     {len(representatives)}")
    print(f"Horas por ZIP final:             {total_hours_per_zip:,}")
    print(f"Ejemplo ZIP final:               {sample_zip!r} (string, 6 dígitos)")
    print(f"Filas finales:                   {total_rows:,}")
    print(f"12 variables solares:            CUMPLIDO")
    print(f"2 datos geográficos:             CUMPLIDO")
    print(f"Duplicados ZIP+datetime:          0 (validado por orden estricto)")
    print(f"Valores solares faltantes:       {final_missing_total:,} (preservados; no imputados)")
    if final_missing_total:
        print("Faltantes por variable:")
        for variable in HOURLY_VARIABLES:
            count = final_missing_by_variable[variable]
            if count:
                print(f"  {variable}: {count:,}")
    print(f"CSV final:                       {output_path.resolve()}")

    summary = {
        "version": APP_VERSION,
        "department": "Antioquia",
        "start_year": start_year,
        "end_year": end_year,
        "original_zip_count": len(zip_codes),
        "representative_zip_count": len(representatives),
        "redundant_zip_count": redundant,
        "hours_per_zip": total_hours_per_zip,
        "final_rows": total_rows,
        "final_csv": str(output_path.resolve()),
        "missing_solar_total": final_missing_total,
        "missing_solar_by_variable": final_missing_by_variable,
        "missing_policy": (
            "Se preservan como celdas vacías los null originales de Open-Meteo; "
            "no se imputan, no se reemplazan por 0 y no se elimina la hora."
        ),
        "groups": groups,
        "per_zip_rows": per_zip_rows,
    }
    _atomic_json(checkpoint_dir / "dataset_final_manifest.json", summary)

    if args.cleanup_checkpoints:
        for child in checkpoint_dir.iterdir():
            if child.name == "dataset_final_manifest.json":
                continue
            if child.is_file():
                child.unlink()
        print("Checkpoints mensuales eliminados después de la validación final.")
    else:
        print(
            "Checkpoints conservados para reanudar/auditar. "
            "NO se entregan al docente."
        )

    return 0

def main(argv: list[str] | None = None) -> int:
    parser = build_parser()

    # Si el docente ejecuta simplemente `python chal.py`, entra en modo guiado.
    # `python chal.py --help` sigue mostrando la ayuda completa.
    effective_argv = sys.argv[1:] if argv is None else argv

    if not effective_argv:
        try:
            effective_argv = interactive_argv()
        except (EOFError, KeyboardInterrupt):
            print("\n[INFO] Ejecución cancelada por el usuario.")
            return 130
        except ChallengeError as exc:
            print(f"[ERROR] {exc}", file=sys.stderr)
            return 2

    args = parser.parse_args(effective_argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(message)s",
        datefmt="%H:%M:%S",
    )

    if args.antioquia_dataset:
        try:
            return run_antioquia_dataset(args)
        except (ChallengeError, OSError, ValueError) as exc:
            print(f"[ERROR] DATASET ANTIOQUIA: {exc}", file=sys.stderr)
            return 2

    if args.show_catalog:
        print_separator("CATÁLOGO DE UBICACIONES INCORPORADO")
        for location in builtin_catalog():
            print(
                f"ZIP {location.zip_code} | "
                f"{location.city} | {location.department} | "
                f"LAT={location.latitude:.6f} "
                f"LON={location.longitude:.6f}"
            )
        return 0

    if not args.start or not args.end:
        parser.error("--start y --end son obligatorios para ejecutar el reto.")

    try:
        validate_dates(args.start, args.end)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))

    try:
        validate_hour_window(args.start_hour, args.end_hour)
    except argparse.ArgumentTypeError as exc:
        parser.error(str(exc))

    if args.buffer_size <= 0:
        parser.error("--buffer-size debe ser > 0")
    if args.timeout <= 0:
        parser.error("--timeout debe ser > 0")
    if args.batch_size <= 0:
        parser.error("--batch-size debe ser > 0")
    if args.api_concurrency <= 0:
        parser.error("--api-concurrency debe ser > 0")
    if args.max_attempts <= 0:
        parser.error("--max-attempts debe ser > 0")
    if args.repetitions <= 0:
        parser.error("--repetitions debe ser > 0")

    try:
        end_date_obj = date.fromisoformat(args.end)
        days_old = (date.today() - end_date_obj).days
        if 0 <= days_old < 5:
            logging.warning(
                "La fecha final está muy cerca de hoy (%d día(s)). "
                "Algunas fuentes históricas de Open-Meteo pueden tener "
                "retraso de disponibilidad; para una evidencia reproducible "
                "conviene usar una ventana más antigua.",
                days_old,
            )
    except ValueError:
        pass

    if not (args.zips or args.cities or args.departments):
        parser.error(
            "Indique al menos uno: --zips, --cities o --departments."
        )

    logical_cores = os.cpu_count() or 1

    print_separator("CONFIGURACIÓN DE EJECUCIÓN")
    print(f"Versión:                     {APP_VERSION}")
    modo_visible = {
        "serial": "secuencial",
        "parallel": "paralelo",
        "both": "ambos (secuencial + paralelo)",
    }.get(args.mode, args.mode)
    print(f"Modo:                        {modo_visible}")
    print(f"Ventana de fechas:           {args.start} -> {args.end}")
    print(f"Ventana horaria:             {format_hour_window(args.start_hour, args.end_hour)}")
    print(f"Zona horaria:                {args.timezone}")
    machine = system_summary()
    print(f"Sistema operativo:           {machine['os']}")
    print(f"Python:                      {machine['python']}")
    print(f"Arquitectura:                {machine['architecture']}")
    print(f"Procesador:                  {machine['processor']}")
    print(f"Núcleos lógicos detectados:  {logical_cores}")
    print("Política de workers:          fija por os.cpu_count()")
    print("Conteo configurable por CLI:  NO")
    print(f"Capacidad del búfer:         {args.buffer_size}")
    print(f"ZIP por lote/request:        {args.batch_size}")
    print(f"Concurrencia API máxima:     {args.api_concurrency}")
    print(f"Intentos HTTP máximos/lote:  {args.max_attempts}")
    print(f"Repeticiones:                {args.repetitions}")
    print(f"Fuente geográfica:           {args.geo_source}")
    print(f"Modo SSL:                    {args.ssl_mode}")
    print("Variables solares (12):       " + ", ".join(HOURLY_VARIABLES))
    print("Alcance del cronometraje:    consulta solar + filtro horario + normalización + agrupación")
    print("Fuera del cronometraje:      resolución geográfica y escritura de CSV")
    print("Métricas:                    aceleración, eficiencia y sobrecosto (overhead)")
    print(
        "Definiciones:                 "
        "S=Ts/Tp | E=S/P*100 | O=P*Tp-Ts"
    )
    if args.geo_source == "openstreet":
        print_separator("ATRIBUCIÓN DE OPENSTREETMAP")
        print("Datos geográficos: colaboradores de OpenStreetMap, ODbL.")
        print(
            "Nominatim se usa únicamente para la resolución geográfica antes del cronometraje; "
            "los datos meteorológicos siguen proviniendo de Open-Meteo."
        )

    print_separator("SELECCIÓN GEOGRÁFICA / ZIP -> LAT-LON")

    try:
        locations, input_duplicates_removed = resolve_requested_locations(args)
    except ChallengeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 2

    for location in locations:
        print(
            f"ZIP={location.zip_code}:{location.country_code} | "
            f"{location.city}, {location.department} | "
            f"LAT={location.latitude:.6f} "
            f"LON={location.longitude:.6f} | "
            f"fuente={location.coordinate_source}"
        )
        if location.coordinate_source.startswith("OpenStreet"):
            print(f"    OSM {bbox_text(location)}")

    print(f"Sitios de entrada únicos:    {len(locations)}")
    print(f"Entradas repetidas eliminadas:{input_duplicates_removed:>3}")
    print(
        "Conjunto de ZIP resuelto:     "
        + ", ".join(location.zip_code for location in locations)
    )

    # Regla literal del docente: el modo paralelo usa EXACTAMENTE todo el
    # presupuesto detectado por os.cpu_count(). El número de workers NO depende
    # de la cantidad de sitios y NO puede modificarse desde la línea de comandos.
    try:
        producer_count, consumer_count = fixed_worker_plan(logical_cores)
    except ChallengeError as exc:
        parser.error(str(exc))

    total_worker_threads = producer_count + consumer_count

    # Invariante auditable: esta condición debe cumplirse antes de ejecutar
    # cualquier hilo paralelo.
    if total_worker_threads != logical_cores:
        raise RuntimeError(
            "Violación de la regla de workers: el total no coincide con os.cpu_count()."
        )

    modalities_per_repetition = 2 if args.mode == "both" else 1
    rows_per_site = expected_rows_per_site(
        args.start,
        args.end,
        args.start_hour,
        args.end_hour,
    )
    raw_rows_per_modality = len(locations) * rows_per_site
    total_rows_processed = (
        raw_rows_per_modality
        * args.repetitions
        * modalities_per_repetition
    )
    batches = make_location_batches(locations, args.batch_size)
    batches_per_modality = len(batches)
    estimated_weather_requests = (
        batches_per_modality
        * args.repetitions
        * modalities_per_repetition
    )
    per_zip_baseline_requests = (
        len(locations)
        * args.repetitions
        * modalities_per_repetition
    )
    avoided_requests = per_zip_baseline_requests - estimated_weather_requests
    reduction_pct = (
        avoided_requests / per_zip_baseline_requests * 100.0
        if per_zip_baseline_requests
        else 0.0
    )

    print_separator("TAMAÑO Y PRESUPUESTO DEL EXPERIMENTO")
    print(f"Sitios/ZIP a consultar:       {len(locations)}")
    print(f"ZIP por lote/request:         {args.batch_size}")
    print(f"Lotes/tareas por modalidad:   {batches_per_modality}")
    print(f"Filas horarias por ZIP:       {rows_per_site}")
    print(f"Filas crudas por modalidad:   {raw_rows_per_modality}")
    print(f"Repeticiones:                 {args.repetitions}")
    print(f"Modalidades por repetición:   {modalities_per_repetition}")
    print(f"Filas procesadas estimadas:   {total_rows_processed}")
    print(f"Requests HTTP meteorológicas estimadas: {estimated_weather_requests}")
    print(f"Baseline 1 request/ZIP:        {per_zip_baseline_requests}")
    print(f"Requests evitadas por batching:{avoided_requests:>4} ({reduction_pct:.2f}%)")
    print(
        "Granularidad de request:        hasta "
        f"{args.batch_size} ZIP/coordenadas por request para TODA la ventana; "
        "no se solicita por hora ni por día"
    )

    if estimated_weather_requests >= 100:
        print(
            "ADVERTENCIA: presupuesto de requests todavía alto. Revise batch-size, "
            "repeticiones y concurrencia antes de ejecutar."
        )

    print_separator("PLAN DE TRABAJADORES Y PARTICIÓN")
    print(f"Núcleos lógicos:             {logical_cores}")
    print(f"Trabajos de sitio disponibles:{len(locations):>3}")
    print(f"Productores efectivos:       {producer_count}")
    print(f"Consumidores efectivos:      {consumer_count}")
    print(f"Total de hilos trabajadores: {total_worker_threads}")
    print(
        "Verificación workers == cores: "
        + ("SÍ" if total_worker_threads == logical_cores else "NO")
    )
    print(
        "Concurrencia HTTP Open-Meteo: "
        f"máximo {min(args.api_concurrency, producer_count)} solicitud(es) simultánea(s)"
    )
    print(
        "Política fija:               productores + consumidores = "
        "TODOS los núcleos lógicos detectados"
    )
    print(
        "Partición lógica:             "
        f"{batches_per_modality} lote(s) de hasta {args.batch_size} ZIP; "
        "NO equivale al número de cores/workers"
    )
    print(
        "Mismo trabajo sec./par.:       mismos sitios, mismos lotes, misma ventana "
        "y mismas variables"
    )

    if args.plan_only:
        print_separator("PLAN-ONLY / PREVUELO")
        print("No se realizaron solicitudes meteorológicas a Open-Meteo.")
        print("La resolución geográfica anterior puede haber consultado 4-72/MinTIC.")
        print("Revise este presupuesto antes de lanzar la ejecución completa.")
        print("Las requests mostradas son HTTP meteorológicas estimadas; los datos siguen siendo los mismos.")
        return 0

    if producer_count > len(locations):
        logging.warning(
            "Hay %d productores para %d sitios: algunos productores no tendrán "
            "trabajo. Se conservan porque la regla exige usar todos los cores.",
            producer_count,
            len(locations),
        )

    if consumer_count > len(locations):
        logging.info(
            "Hay %d consumidores para %d sitios. Esta configuración también "
            "prueba el cierre ordenado de consumidores sin trabajo.",
            consumer_count,
            len(locations),
        )

    print_separator("CONSISTENCIA DEL TRABAJO")
    print(
        "Secuencial y paralelo usarán el MISMO conjunto de ZIP resuelto, "
        "mismos LAT/LON, misma ventana de fecha/hora, misma zona horaria y "
        "mismas variables meteorológicas."
    )
    print(
        "No se introducen retardos artificiales en el pipeline medido."
    )
    print(
        "Secuencial y paralelo usan la MISMA política de reintentos; "
        "todo reintento forma parte del tiempo medido."
    )

    print_separator("CUATRO REGLAS CRÍTICAS - VERIFICACIÓN DEL DOCENTE")
    workers_rule_ok = (
        total_worker_threads == logical_cores
        and producer_count >= 1
        and consumer_count >= 1
    )
    print(
        "1) Todos los cores disponibles: "
        + ("CUMPLIDO" if workers_rule_ok else "NO CUMPLIDO")
        + f" ({total_worker_threads} workers == {logical_cores} núcleos lógicos; "
        "conteo no configurable)"
    )
    print("2) Mismo trabajo sec./par.:     CUMPLIDO")
    print("3) Sin retardos artificiales:   CUMPLIDO en el pipeline medido")
    print("4) Resultados idénticos:        PENDIENTE - se verifica al finalizar")

    serial_runs: list[ExecutionResult] = []
    parallel_runs: list[ExecutionResult] = []
    pair_validity: list[bool] = []
    pair_orders: list[str] = []

    print_separator("PLAN EXPERIMENTAL")
    print(
        f"Repeticiones solicitadas:       {args.repetitions}"
    )
    if args.mode == "both":
        print(
            "Orden alternado:                "
            "impares SECUENCIAL->PARALELO; pares PARALELO->SECUENCIAL"
        )
    print(
        "Objetivo:                        reducir sesgo por orden, caché, "
        "latencia y carga momentánea del sistema"
    )

    for repetition in range(1, args.repetitions + 1):
        print_separator(
            f"REPETICIÓN {repetition}/{args.repetitions}"
        )

        if args.mode == "both":
            serial_first = repetition % 2 == 1
            order = (
                "SECUENCIAL -> PARALELO"
                if serial_first
                else "PARALELO -> SECUENCIAL"
            )
            pair_orders.append(order)
            print(f"Orden: {order}")

            if serial_first:
                print("[1/2] Ejecutando modalidad secuencial...")
                serial_run = serial_result_from_call(
                    locations,
                    args,
                )
                print(
                    f"      Tiempo secuencial: {serial_run.elapsed:.6f} s"
                )

                print("[2/2] Ejecutando modalidad paralela...")
                parallel_run = parallel_result_from_call(
                    locations,
                    args,
                    producer_count,
                    consumer_count,
                )
                print(
                    f"      Tiempo paralelo:   {parallel_run.elapsed:.6f} s"
                )
            else:
                print("[1/2] Ejecutando modalidad paralela...")
                parallel_run = parallel_result_from_call(
                    locations,
                    args,
                    producer_count,
                    consumer_count,
                )
                print(
                    f"      Tiempo paralelo:   {parallel_run.elapsed:.6f} s"
                )

                print("[2/2] Ejecutando modalidad secuencial...")
                serial_run = serial_result_from_call(
                    locations,
                    args,
                )
                print(
                    f"      Tiempo secuencial: {serial_run.elapsed:.6f} s"
                )

            serial_runs.append(serial_run)
            parallel_runs.append(parallel_run)

            same_output = same_execution_output(
                serial_run,
                parallel_run,
            )
            pair_valid = (
                bool(serial_run.rows)
                and bool(parallel_run.rows)
                and not serial_run.errors
                and not parallel_run.errors
                and same_output
            )
            pair_validity.append(pair_valid)

            rep_speedup = (
                serial_run.elapsed / parallel_run.elapsed
                if parallel_run.elapsed > 0
                else 0.0
            )

            print(
                "      Resultado sec. == par.: "
                + ("SÍ" if same_output else "NO")
            )

            if not same_output:
                print("      DIAGNÓSTICO DE DIFERENCIA:")
                for detail in diagnose_execution_difference(
                    serial_run,
                    parallel_run,
                ):
                    print(f"        - {detail}")

            print(
                "      Repetición válida:       "
                + ("SÍ" if pair_valid else "NO")
            )
            print(
                f"      Aceleración repetición:  {rep_speedup:.6f}x"
            )

        elif args.mode == "serial":
            serial_run = serial_result_from_call(
                locations,
                args,
            )
            serial_runs.append(serial_run)
            print(
                f"Tiempo secuencial: {serial_run.elapsed:.6f} s"
            )

        else:
            parallel_run = parallel_result_from_call(
                locations,
                args,
                producer_count,
                consumer_count,
            )
            parallel_runs.append(parallel_run)
            print(
                f"Tiempo paralelo: {parallel_run.elapsed:.6f} s"
            )

    print_separator("VALIDACIÓN EXPERIMENTAL")

    serial_consistent = (
        repetitions_consistent(serial_runs)
        if serial_runs
        else True
    )
    parallel_consistent = (
        repetitions_consistent(parallel_runs)
        if parallel_runs
        else True
    )

    if args.mode == "both":
        all_pairs_valid = (
            len(pair_validity) == args.repetitions
            and all(pair_validity)
        )
        valid_experiment = (
            all_pairs_valid
            and serial_consistent
            and parallel_consistent
        )
    elif args.mode == "serial":
        valid_experiment = (
            bool(serial_runs)
            and all(run.rows and not run.errors for run in serial_runs)
            and serial_consistent
        )
    else:
        valid_experiment = (
            bool(parallel_runs)
            and all(run.rows and not run.errors for run in parallel_runs)
            and parallel_consistent
        )

    print(
        "Consistencia entre repeticiones sec.: "
        + ("SÍ" if serial_consistent else "NO")
    )
    print(
        "Consistencia entre repeticiones par.: "
        + ("SÍ" if parallel_consistent else "NO")
    )

    if args.mode == "both":
        print(
            f"Pares secuencial/paralelo válidos:  "
            f"{sum(pair_validity)}/{args.repetitions}"
        )

    all_serial_errors = [
        error
        for run in serial_runs
        for error in run.errors
    ]
    all_parallel_errors = [
        error
        for run in parallel_runs
        for error in run.errors
    ]

    print(f"Errores secuenciales totales:      {len(all_serial_errors)}")
    print(f"Errores paralelos totales:         {len(all_parallel_errors)}")
    print(
        "Experimento completo válido:        "
        + ("SÍ" if valid_experiment else "NO")
    )
    if args.mode == "both":
        print(
            "Regla crítica #4:                  "
            + (
                "CUMPLE en todas las repeticiones"
                if valid_experiment
                else "NO CUMPLE completamente"
            )
        )

    print_separator("RESULTADOS POR REPETICIÓN")

    if args.mode == "both":
        print(
            "Rep | Orden                     | Tsecuencial(s) | "
            "Tparalelo(s) | Speedup | Válida"
        )
        print("-" * 92)

        for index, (
            serial_run,
            parallel_run,
            order,
            valid,
        ) in enumerate(
            zip(
                serial_runs,
                parallel_runs,
                pair_orders,
                pair_validity,
            ),
            start=1,
        ):
            speedup = (
                serial_run.elapsed / parallel_run.elapsed
                if parallel_run.elapsed > 0
                else 0.0
            )
            print(
                f"{index:>3} | {order:<25} | "
                f"{serial_run.elapsed:>13.6f} | "
                f"{parallel_run.elapsed:>11.6f} | "
                f"{speedup:>7.4f} | "
                f"{'SÍ' if valid else 'NO'}"
            )

    elif args.mode == "serial":
        for index, run in enumerate(serial_runs, start=1):
            print(
                f"Repetición {index}: {run.elapsed:.6f} s "
                f"| {'VÁLIDA' if not run.errors else 'CON ERROR'}"
            )
    else:
        for index, run in enumerate(parallel_runs, start=1):
            print(
                f"Repetición {index}: {run.elapsed:.6f} s "
                f"| {'VÁLIDA' if not run.errors else 'CON ERROR'}"
            )

    print_separator("RENDIMIENTO ESTADÍSTICO")

    serial_stats = timing_stats(
        [run.elapsed for run in serial_runs]
    )
    parallel_stats = timing_stats(
        [run.elapsed for run in parallel_runs]
    )

    if serial_runs:
        print(
            "Secuencial - promedio/mediana/desv.: "
            f"{serial_stats['mean']:.6f} / "
            f"{serial_stats['median']:.6f} / "
            f"{serial_stats['stdev']:.6f} s"
        )
        print(
            "Secuencial - mínimo/máximo:          "
            f"{serial_stats['min']:.6f} / "
            f"{serial_stats['max']:.6f} s"
        )

    if parallel_runs:
        print(
            "Paralelo   - promedio/mediana/desv.: "
            f"{parallel_stats['mean']:.6f} / "
            f"{parallel_stats['median']:.6f} / "
            f"{parallel_stats['stdev']:.6f} s"
        )
        print(
            "Paralelo   - mínimo/máximo:          "
            f"{parallel_stats['min']:.6f} / "
            f"{parallel_stats['max']:.6f} s"
        )

    if (
        args.mode == "both"
        and valid_experiment
        and parallel_stats["mean"] > 0
    ):
        speedup_global = (
            serial_stats["mean"]
            / parallel_stats["mean"]
        )
        efficiency = (
            speedup_global / total_worker_threads * 100.0
        )
        cost = (
            total_worker_threads * parallel_stats["mean"]
        )
        overhead = cost - serial_stats["mean"]

        print(
            f"Aceleración global S=Ts/Tp:          "
            f"{speedup_global:.6f}x"
        )
        print(
            f"Eficiencia E=S/P*100 (P={total_worker_threads}): "
            f"{efficiency:.2f}%"
        )
        print(
            f"Costo paralelo C=P*Tp:                "
            f"{cost:.6f} s-hilo"
        )
        print(
            f"Sobrecosto (overhead) O=P*Tp-Ts:                   "
            f"{overhead:.6f} s-hilo"
        )
        print(
            "Nota: eficiencia y sobrecosto describen esta configuración "
            "de hilos y una carga dominada por E/S; no equivalen a uso "
            "físico porcentual de CPU."
        )
    elif args.mode == "both":
        print(
            "Aceleración/eficiencia/sobrecosto: N/D porque el "
            "experimento completo no fue válido."
        )

    # Resultado canónico para agrupación y escritura.
    if parallel_runs:
        chosen_run = parallel_runs[-1]
    else:
        chosen_run = serial_runs[-1]

    chosen_groups = chosen_run.groups
    chosen_rows = chosen_run.rows

    print_data_integrity(
        chosen_groups,
        chosen_rows,
        locations,
        args,
    )

    print_request_summary(
        serial_runs,
        parallel_runs,
        args.api_concurrency,
        args.max_attempts,
    )

    if parallel_runs:
        print_sync_summary(
            parallel_runs,
            len(parallel_runs),
        )

    print_grouping_summary(
        chosen_groups,
        input_sites=len(locations),
    )

    print_engineering_conclusions(
        valid_experiment,
        serial_runs,
        parallel_runs,
        total_worker_threads,
        logical_cores,
        locations,
    )

    serial_errors = all_serial_errors
    parallel_errors = all_parallel_errors

    # No sobreescribir evidencia válida si la ejecución falló.
    print_separator("SALIDA")
    print(
        "Formato principal:              group_id + grouped_zip_codes + datetime + "
        "2 datos geográficos + 12 variables de radiación solar"
    )
    print("Datos geográficos:               representative_latitude, representative_longitude")
    print("Variables solares:                " + ", ".join(HOURLY_VARIABLES))
    print("Agrupación/trazabilidad:         zipcode conserva un ZIP representativo; miembros se reportan en consola")
    print("Salida solicitada:               un único CSV final")

    output_path = Path(args.output)
    csv_delimiter = ";" if args.csv_delimiter == "semicolon" else ","

    if valid_experiment and chosen_rows:
        # Auditoría final del requisito del docente: ZIP como STRING.
        for row in chosen_rows:
            zip_value = row["zipcode"]
            if not isinstance(zip_value, str):
                raise DataValidationError(
                    f"zipcode debe ser str, no {type(zip_value).__name__}"
                )
            if not re.fullmatch(r"\d{6}", zip_value):
                raise DataValidationError(
                    f"zipcode colombiano debe conservar 6 dígitos: {zip_value!r}"
                )

        print("Validación zipcode como str:       CUMPLIDO")
        print(f"Ejemplo zipcode:                  {chosen_rows[0]['zipcode']!r} (tipo=str)")

        write_csv(
            output_path,
            FINAL_CSV_FIELDS,
            chosen_rows,
            delimiter=csv_delimiter,
        )

        print(f"CSV final GEO2+SOLAR12:           {output_path.resolve()}")
        print(
            f"Delimitador CSV:                  {args.csv_delimiter} "
            f"({csv_delimiter!r})"
        )
        print(
            "Se genera un único CSV: zipcode representativo como string de 6 dígitos "
            "+ latitud/longitud representativas + datetime + 12 variables de radiación solar."
        )
    else:
        print(
            "CSV NO ESCRITO: errores de ejecución o ausencia de datos válidos harían "
            "inválido el resultado experimental."
        )

    all_errors = serial_errors + parallel_errors

    if all_errors:
        print()
        print("Errores detectados:")
        for error in all_errors:
            print(f"  - {error}")

    return 0 if valid_experiment else 2


if __name__ == "__main__":
    raise SystemExit(main())
