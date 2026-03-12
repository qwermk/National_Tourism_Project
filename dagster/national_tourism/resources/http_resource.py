# =============================================================================
# HTTP Resource — Cliente HTTP para fuentes de datos externas
# =============================================================================
# Recurso configurable para consumir APIs REST y descargar archivos CSV/Excel.
# Incluye reintentos con backoff exponencial y timeout configurable.
#
# Fuentes soportadas:
#   - World Bank Open Data API (JSON)
#   - datos.gov.co / CKAN API (CSV/JSON)
#   - Cualquier URL HTTP(S) que devuelva CSV, JSON o Excel
# =============================================================================

import io
import time
from typing import Any

import pandas as pd
import requests
from dagster import ConfigurableResource, get_dagster_logger

logger = get_dagster_logger()

_DEFAULT_USER_AGENT = (
    "NationalTourismPipeline/1.0 (Dagster; datos-abiertos-colombia; "
    "github.com/qwermk/National_Tourism_Project)"
)


class HttpResource(ConfigurableResource):
    """
    Resource HTTP basado en `requests` para consumir APIs externas.

    Configuración (variables de entorno o parámetros Dagster):
      HTTP_TIMEOUT_SECONDS   — segundos de espera por request (default: 30)
      HTTP_MAX_RETRIES       — reintentos en caso de error (default: 3)
      HTTP_RETRY_DELAY       — segundos entre reintentos (default: 2.0)
    """

    timeout_seconds: int = 30
    max_retries: int = 3
    retry_delay_seconds: float = 2.0
    user_agent: str = _DEFAULT_USER_AGENT

    # ------------------------------------------------------------------
    # Interno
    # ------------------------------------------------------------------

    def _session(self) -> requests.Session:
        s = requests.Session()
        s.headers.update({"User-Agent": self.user_agent})
        return s

    def _get_raw(self, url: str, params: dict | None = None) -> requests.Response:
        """GET con reintentos exponenciales. Lanza HTTPError en 4xx/5xx."""
        session = self._session()
        last_exc: Exception | None = None

        for attempt in range(1, self.max_retries + 1):
            try:
                resp = session.get(url, params=params, timeout=self.timeout_seconds)
                resp.raise_for_status()
                return resp
            except (requests.ConnectionError, requests.Timeout) as exc:
                last_exc = exc
                if attempt < self.max_retries:
                    wait = self.retry_delay_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        f"[HttpResource] Intento {attempt}/{self.max_retries} fallido "
                        f"({url}): {exc}. Esperando {wait:.1f}s..."
                    )
                    time.sleep(wait)
            except requests.HTTPError as exc:
                raise  # No reintentar en errores 4xx/5xx

        raise last_exc  # type: ignore[misc]

    # ------------------------------------------------------------------
    # API pública
    # ------------------------------------------------------------------

    def get_json(self, url: str, params: dict | None = None) -> Any:
        """Descarga y parsea JSON desde una URL."""
        resp = self._get_raw(url, params=params)
        return resp.json()

    def get_text(self, url: str, params: dict | None = None) -> str:
        """Descarga el cuerpo de la respuesta como texto."""
        resp = self._get_raw(url, params=params)
        return resp.text

    def get_dataframe_from_csv(
        self,
        url: str,
        params: dict | None = None,
        encoding: str = "utf-8",
        **read_csv_kwargs: Any,
    ) -> pd.DataFrame:
        """
        Descarga un archivo CSV desde ``url`` y lo devuelve como DataFrame.

        Parámetros adicionales se pasan directamente a ``pd.read_csv``.
        """
        resp = self._get_raw(url, params=params)
        content = resp.content
        return pd.read_csv(
            io.BytesIO(content),
            encoding=encoding,
            encoding_errors="replace",
            **read_csv_kwargs,
        )

    def get_dataframe_from_excel(
        self,
        url: str,
        params: dict | None = None,
        **read_excel_kwargs: Any,
    ) -> pd.DataFrame:
        """
        Descarga un archivo Excel (.xlsx / .xls) y lo devuelve como DataFrame.

        Parámetros adicionales se pasan directamente a ``pd.read_excel``.
        """
        resp = self._get_raw(url, params=params)
        return pd.read_excel(io.BytesIO(resp.content), **read_excel_kwargs)


# ---------------------------------------------------------------------------
# Instancia singleton que se inyecta en las Definitions
# ---------------------------------------------------------------------------
http_resource = HttpResource()
