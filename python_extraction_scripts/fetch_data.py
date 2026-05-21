"""
fetch_data.py — Coleta de dados climáticos do Open-Meteo ERA5

Salva cada chunk individualmente em data/chunks/.
Pode ser interrompido e re-executado a qualquer momento —
chunks já baixados são pulados automaticamente.

Uso:
  python fetch_data.py            # fetch (pula chunks já salvos)
  python fetch_data.py --combine  # só combina chunks existentes em CSVs finais
  python fetch_data.py --clean    # limpa tudo e recomeça

Requer: pip install requests pandas
"""

import os
import sys
import time
import glob
import shutil
import requests
import pandas as pd
from pathlib import Path

# ── Configuração ─────────────────────────────────────────────

OUTPUT_DIR = Path("data")
CHUNK_DIR = OUTPUT_DIR / "chunks"

CHUNK_YEARS = 2
DELAY_CHUNK = 8           # segundos entre chunks
DELAY_CITY = 20            # segundos entre cidades
DELAY_429 = 45             # base para backoff em 429
MAX_RETRIES = 6
REQUEST_TIMEOUT = 120      # timeout do request em segundos

DAILY_VARS = (
    "temperature_2m_max,temperature_2m_min,temperature_2m_mean,"
    "precipitation_sum,windspeed_10m_max,shortwave_radiation_sum"
)

CITIES = [
    {"city": "Manaus",          "region": "Norte",         "lat": -3.10,  "lon": -60.02},
    {"city": "Belém",           "region": "Norte",         "lat": -1.46,  "lon": -48.48},
    {"city": "Recife",          "region": "Nordeste",      "lat": -8.05,  "lon": -34.88},
    {"city": "Fortaleza",       "region": "Nordeste",      "lat": -3.72,  "lon": -38.54},
    {"city": "Salvador",        "region": "Nordeste",      "lat": -12.97, "lon": -38.51},
    {"city": "Brasília",        "region": "Centro-Oeste",  "lat": -15.78, "lon": -47.93},
    {"city": "Cuiabá",          "region": "Centro-Oeste",  "lat": -15.60, "lon": -56.10},
    {"city": "São Paulo",       "region": "Sudeste",       "lat": -23.55, "lon": -46.63},
    {"city": "Rio de Janeiro",  "region": "Sudeste",       "lat": -22.91, "lon": -43.18},
    {"city": "Belo Horizonte",  "region": "Sudeste",       "lat": -19.92, "lon": -43.94},
    {"city": "Porto Alegre",    "region": "Sul",           "lat": -30.03, "lon": -51.23},
    {"city": "Curitiba",        "region": "Sul",           "lat": -25.43, "lon": -49.27},
]

START_YEAR = 1980
END_YEAR = 2024

# ── Helpers ──────────────────────────────────────────────────

def slug(name: str) -> str:
    return (name.lower()
            .replace(" ", "_")
            .replace("ã", "a").replace("á", "a")
            .replace("é", "e").replace("í", "i"))


def chunk_filename(city_name: str, start: str, end: str) -> Path:
    return CHUNK_DIR / f"{slug(city_name)}_{start}_{end}.csv"


def date_ranges():
    """Gera lista de (start_date, end_date) strings."""
    ranges = []
    for y in range(START_YEAR, END_YEAR + 1, CHUNK_YEARS):
        y_end = min(y + CHUNK_YEARS - 1, END_YEAR)
        ranges.append((f"{y}-01-01", f"{y_end}-12-31"))
    return ranges


def fetch_one_chunk(lat, lon, start, end):
    """Busca um chunk com retry agressivo para 429."""
    url = (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        f"&daily={DAILY_VARS}&timezone=auto"
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=REQUEST_TIMEOUT)

            if resp.status_code == 429:
                wait = DELAY_429 * attempt
                print(f"        429 — aguardando {wait}s ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue

            resp.raise_for_status()
            data = resp.json()
            daily = data["daily"]

            df = pd.DataFrame({
                "date":      pd.to_datetime(daily["time"]),
                "temp_max":  daily["temperature_2m_max"],
                "temp_min":  daily["temperature_2m_min"],
                "temp_mean": daily["temperature_2m_mean"],
                "precip":    daily["precipitation_sum"],
                "wind_max":  daily["windspeed_10m_max"],
                "radiation": daily["shortwave_radiation_sum"],
            })
            return df.dropna()

        except requests.exceptions.HTTPError as e:
            if resp.status_code == 429:
                wait = DELAY_429 * attempt
                print(f"        429 — aguardando {wait}s ({attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            wait = 15 * attempt
            print(f"        Erro: {e} — retry em {wait}s ({attempt}/{MAX_RETRIES})")
            time.sleep(wait)

    return None  # falhou todas as tentativas


# ── Fetch ────────────────────────────────────────────────────

def fetch_all():
    CHUNK_DIR.mkdir(parents=True, exist_ok=True)
    ranges = date_ranges()

    for i, city in enumerate(CITIES):
        name = city["city"]
        print(f"\n[{i+1:2d}/12] {name}")

        city_done = True
        for start, end in ranges:
            fpath = chunk_filename(name, start, end)

            if fpath.exists():
                n = len(pd.read_csv(fpath))
                print(f"    ✓ {start} → {end}  (cache: {n} dias)")
                continue

            city_done = False
            df = fetch_one_chunk(city["lat"], city["lon"], start, end)

            if df is not None and len(df) > 0:
                df["city"] = name
                df["region"] = city["region"]
                df.to_csv(fpath, index=False)
                print(f"    ✓ {start} → {end}  ({len(df):,} dias) → salvo")
            else:
                print(f"    ✗ {start} → {end}  FALHOU")

            time.sleep(DELAY_CHUNK)

        if city_done:
            print(f"    (todos os chunks em cache)")

        # Pausa entre cidades
        if i < len(CITIES) - 1 and not city_done:
            print(f"    ⏳ {DELAY_CITY}s até próxima cidade...")
            time.sleep(DELAY_CITY)


# ── Combine ──────────────────────────────────────────────────

def combine():
    OUTPUT_DIR.mkdir(exist_ok=True)
    chunk_files = sorted(CHUNK_DIR.glob("*.csv"))

    if not chunk_files:
        print("Nenhum chunk encontrado em data/chunks/")
        return

    print(f"\nCombinando {len(chunk_files)} chunks...")

    all_dfs = []
    for f in chunk_files:
        df = pd.read_csv(f, parse_dates=["date"])
        all_dfs.append(df)

    brazil_daily = pd.concat(all_dfs, ignore_index=True)

    # ── SP daily (Parte I) ──
    sp_daily = brazil_daily[brazil_daily["city"] == "São Paulo"].copy()
    sp_daily_out = sp_daily.drop(columns=["city", "region"])
    sp_daily_out = sp_daily_out.sort_values("date").reset_index(drop=True)
    sp_daily_out.to_csv(OUTPUT_DIR / "sp_daily.csv", index=False)

    # ── Brazil monthly (Parte II–IV) ──
    brazil_daily["year"] = brazil_daily["date"].dt.year
    brazil_daily["month"] = brazil_daily["date"].dt.month

    monthly = brazil_daily.groupby(["city", "region", "year", "month"]).agg(
        temp=("temp_mean", "mean"),
        temp_max=("temp_max", "mean"),
        temp_min=("temp_min", "mean"),
        precip=("precip", "sum"),
        wind=("wind_max", "mean"),
        radiation=("radiation", "mean"),
    ).reset_index()

    monthly.to_csv(OUTPUT_DIR / "brazil_monthly.csv", index=False)

    # ── Resumo ──
    loaded = monthly["city"].nunique()
    all_cities = {c["city"] for c in CITIES}
    present = set(monthly["city"].unique())
    missing = all_cities - present

    print(f"\n{'=' * 55}")
    print(f"  RESUMO")
    print(f"{'=' * 55}")
    print(f"  sp_daily.csv:        {len(sp_daily_out):,} linhas  ({os.path.getsize(OUTPUT_DIR / 'sp_daily.csv') / 1024:.0f} KB)")
    print(f"  brazil_monthly.csv:  {len(monthly):,} linhas  ({os.path.getsize(OUTPUT_DIR / 'brazil_monthly.csv') / 1024:.0f} KB)")
    print(f"  Cidades:             {loaded}/12")

    if missing:
        print(f"  ⚠ Faltando: {', '.join(sorted(missing))}")
        print(f"    → Re-execute 'python fetch_data.py' para buscar as faltantes")
    else:
        print(f"  ✓ Todas as 12 cidades completas")

    # Chunks por cidade
    print(f"\n  Chunks por cidade:")
    for c in CITIES:
        n_chunks = len(list(CHUNK_DIR.glob(f"{slug(c['city'])}*.csv")))
        n_expected = len(date_ranges())
        status = "✓" if n_chunks == n_expected else f"⚠ {n_chunks}/{n_expected}"
        print(f"    {c['city']:20s} {status}")
    print()


# ── Main ─────────────────────────────────────────────────────

def main():
    if "--clean" in sys.argv:
        if CHUNK_DIR.exists():
            shutil.rmtree(CHUNK_DIR)
        for f in ["sp_daily.csv", "brazil_monthly.csv"]:
            p = OUTPUT_DIR / f
            if p.exists():
                p.unlink()
        print("Cache e CSVs limpos.\n")

    if "--combine" in sys.argv:
        combine()
        return

    print("=" * 55)
    print("  Open-Meteo ERA5 — Fetch incremental")
    print("  Chunks salvos em data/chunks/")
    print("  Pode interromper (Ctrl+C) e re-executar a qualquer momento")
    print("=" * 55)

    fetch_all()
    combine()


if __name__ == "__main__":
    main()
