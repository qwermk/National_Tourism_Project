"""
scripts/dagster_api.py

Lanza la materialización de assets en la instancia Dagster que corre en Docker
Compose, para que los runs queden visibles en la UI (http://localhost:3000).

Las pruebas de integración (test_integration_minio.py) usan dagster.materialize()
que ejecuta en proceso y NO registra runs en el servidor. Este script sí lo hace.

Arquitectura Medallion — flujo de datos en MinIO:
  raw/       ← CSV original tal como viene de la fuente (guardado por los assets)
  bronze/    ← Parquet tipado (salida de los ingestion assets)
  silver/    ← Parquet limpio (salida de los staging assets: stg_*)
  gold/      ← Tablas de negocio agregadas (salida de los mart assets: fct_*, dim_*)

Uso:
    python scripts/dagster_api.py              # pipeline completo Bronze→Silver→Gold
    python scripts/dagster_api.py --layer bronze   # solo ingesta
    python scripts/dagster_api.py --layer silver   # solo staging (requiere bronze)
    python scripts/dagster_api.py --layer gold     # solo marts (requiere silver)
    python scripts/dagster_api.py --reload     # recarga código sin correr nada
    python scripts/dagster_api.py --asset raw_world_bank_arrivals

Requiere: docker-compose corriendo (docker compose -f docker/docker-compose.yml up)
"""

import argparse
import sys
import time

import requests

DAGSTER_URL = "http://localhost:3000"
LOCATION   = "national_tourism.definitions"

# Asset keys por capa — rutas con prefijo de grupo tal como las registra Dagster
LAYER_ASSETS = {
    "bronze": [
        "raw_tourism_arrivals",
        "raw_hotel_occupancy",
        "raw_world_bank_arrivals",
        "raw_citur_arrivals",
        "raw_citur_hotel_occupancy",
    ],
    "silver": [
        "staging/stg_tourism_arrivals",
        "staging/stg_hotel_occupancy",
    ],
    "gold": [
        "gold/fct_tourism_arrivals",
        "gold/fct_hotel_occupancy",
        "gold/dim_departments",
        "gold/dim_date",
    ],
}

ALL_ASSETS = LAYER_ASSETS["bronze"] + LAYER_ASSETS["silver"] + LAYER_ASSETS["gold"]


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------

def gql(query: str, variables: dict | None = None) -> dict:
    resp = requests.post(
        f"{DAGSTER_URL}/graphql",
        json={"query": query, "variables": variables or {}},
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    if not resp.ok:
        print(f"[!] HTTP {resp.status_code}: {resp.text[:500]}")
        resp.raise_for_status()
    data = resp.json()
    if data.get("errors"):
        for err in data["errors"]:
            print(f"[!] GraphQL error: {err['message']}")
        raise RuntimeError("GraphQL errors")
    return data


# ---------------------------------------------------------------------------
# Reload code location
# ---------------------------------------------------------------------------

RELOAD_MUTATION = """
mutation Reload($location: String!) {
  reloadRepositoryLocation(repositoryLocationName: $location) {
    __typename
    ... on WorkspaceLocationEntry {
      name
      loadStatus
    }
    ... on PythonError {
      message
    }
  }
}
"""


def reload_location() -> bool:
    print(f"[→] Recargando código en Dagster ({LOCATION})...")
    data = gql(RELOAD_MUTATION, {"location": LOCATION})
    result = data["data"]["reloadRepositoryLocation"]
    if result["__typename"] == "PythonError":
        print(f"[✗] Error: {result['message']}")
        return False
    status = result.get("loadStatus", "?")
    print(f"[✓] Repositorio recargado — estado: {status}")
    time.sleep(2)  # deja que el webserver termine de cargar
    return True


# ---------------------------------------------------------------------------
# Fetch repository + implicit asset job
# ---------------------------------------------------------------------------

WORKSPACE_QUERY = """
{
  workspaceOrError {
    ... on Workspace {
      locationEntries {
        name
        loadStatus
        locationOrLoadError {
          ... on RepositoryLocation {
            name
            repositories {
              name
              pipelines { name }
            }
          }
          ... on PythonError { message }
        }
      }
    }
  }
}
"""


def get_repo_info() -> tuple[str, str, str] | None:
    """Retorna (location_name, repo_name, job_name) del job implícito de assets."""
    data = gql(WORKSPACE_QUERY)
    entries = data["data"]["workspaceOrError"]["locationEntries"]
    for entry in entries:
        if entry["name"] != LOCATION:
            continue
        loc = entry["locationOrLoadError"]
        if "message" in loc:
            print(f"[✗] Error cargando código: {loc['message'][:300]}")
            return None
        for repo in loc["repositories"]:
            # El job implícito de assets se llama __ASSET_JOB o __ASSET_JOB_N
            asset_jobs = [p["name"] for p in repo["pipelines"] if "__ASSET_JOB" in p["name"]]
            if asset_jobs:
                job = asset_jobs[0]
                print(f"[i] Repositorio: {repo['name']}  |  Job: {job}")
                return LOCATION, repo["name"], job
    print("[✗] No se encontró el job de assets.")
    return None


# ---------------------------------------------------------------------------
# Launch run
# ---------------------------------------------------------------------------

LAUNCH_MUTATION = (
    "mutation LaunchRun($params: ExecutionParams!) {"
    "  launchRun(executionParams: $params) {"
    "    __typename"
    "    ... on LaunchRunSuccess { run { runId status pipelineName } }"
    "    ... on PipelineNotFoundError  { message }"
    "    ... on InvalidSubsetError     { message }"
    "    ... on RunConfigValidationInvalid { errors { message } }"
    "    ... on PythonError { message }"
    "  }"
    "}"
)


def launch_asset_run(location: str, repo: str, job: str, asset_keys: list[str]) -> str | None:
    """Lanza un run de materialización y retorna el runId.

    Los asset keys con '/' se convierten en paths multi-segmento, ej:
      'staging/stg_tourism_arrivals' → {"path": ["staging", "stg_tourism_arrivals"]}
    """
    params = {
        "selector": {
            "repositoryLocationName": location,
            "repositoryName": repo,
            "pipelineName": job,
            "assetSelection": [{"path": key.split("/")} for key in asset_keys],
        },
        "runConfigData": {},
        "executionMetadata": {"tags": []},
    }
    data = gql(LAUNCH_MUTATION, {"params": params})
    result = data["data"]["launchRun"]
    if result["__typename"] == "LaunchRunSuccess":
        run = result["run"]
        print(f"[✓] Run lanzado: {run['runId']}  |  status: {run['status']}")
        return run["runId"]
    else:
        print(f"[✗] Error lanzando run ({result['__typename']}): {result.get('message', result)}")
        return None


# ---------------------------------------------------------------------------
# Poll run status
# ---------------------------------------------------------------------------

RUN_STATUS_QUERY = """
query RunStatus($runId: ID!) {
  runOrError(runId: $runId) {
    ... on Run {
      runId
      status
      stats {
        ... on RunStatsSnapshot {
          startTime
          endTime
          stepsFailed
          stepsSucceeded
        }
      }
    }
    ... on PythonError { message }
  }
}
"""

TERMINAL_STATUSES = {"SUCCESS", "FAILURE", "CANCELED"}


def wait_for_run(run_id: str, poll_interval: float = 3.0, timeout: float = 300.0) -> str:
    """Espera a que el run termine y retorna el status final."""
    deadline = time.time() + timeout
    print(f"[…] Esperando run {run_id[:8]}...", end="", flush=True)
    while time.time() < deadline:
        data = gql(RUN_STATUS_QUERY, {"runId": run_id})
        run = data["data"]["runOrError"]
        status = run.get("status", "UNKNOWN")
        if status in TERMINAL_STATUSES:
            stats = run.get("stats", {})
            ok = stats.get("stepsSucceeded", 0)
            fail = stats.get("stepsFailed", 0)
            print(f"\n[✓] Run terminado — status: {status}  (ok={ok}, fail={fail})")
            return status
        print(".", end="", flush=True)
        time.sleep(poll_interval)
    print(f"\n[!] Timeout esperando run {run_id}")
    return "TIMEOUT"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main():
    parser = argparse.ArgumentParser(description="Lanza assets en Dagster (UI visible)")
    parser.add_argument("--reload", action="store_true", help="Solo recarga el código")
    parser.add_argument(
        "--layer",
        choices=["bronze", "silver", "gold"],
        help="Capa a materializar (default: pipeline completo bronze→silver→gold)",
    )
    parser.add_argument("--asset", nargs="*", help="Asset key(s) específicos a materializar")
    parser.add_argument("--no-wait", action="store_true", help="No esperar a que termine el run")
    args = parser.parse_args()

    # 1. Siempre recargar primero para tener el código más reciente
    if not reload_location():
        sys.exit(1)

    if args.reload:
        return

    # 2. Obtener info del repositorio
    info = get_repo_info()
    if not info:
        print("[!] Reintentando después de 3s...")
        time.sleep(3)
        info = get_repo_info()
    if not info:
        sys.exit(1)

    location, repo, job = info

    # 3. Seleccionar assets según el modo
    if args.asset:
        asset_keys = args.asset
    elif args.layer:
        asset_keys = LAYER_ASSETS[args.layer]
    else:
        # Pipeline completo: bronze → silver → gold en un solo run
        # Dagster respeta las dependencias automáticamente
        asset_keys = ALL_ASSETS

    print(f"\n[→] Materializando {len(asset_keys)} asset(s):")
    for key in asset_keys:
        print(f"      {key}")
    print()

    # 4. Lanzar run
    run_id = launch_asset_run(location, repo, job, asset_keys)
    if not run_id:
        sys.exit(1)

    print(f"[i] Ver en la UI: {DAGSTER_URL}/runs/{run_id}")

    # 5. Esperar resultado
    if not args.no_wait:
        status = wait_for_run(run_id, timeout=600.0)
        if status != "SUCCESS":
            sys.exit(1)


if __name__ == "__main__":
    main()
