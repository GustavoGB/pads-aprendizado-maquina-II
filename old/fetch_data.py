"""
fetch_data.py — Coleta de dados climáticos do Open-Meteo ERA5
Gera dois CSVs:
  - data/sp_daily.csv        → São Paulo diário (Parte I)
  - data/brazil_monthly.csv  → 12 cidades mensal (Parte II–IV)

Salva progresso incremental em data/tmp/ — pode ser re-executado
sem re-baixar cidades já concluídas.

Uso:
  python fetch_data.py          # fetch completo (pula cidades já salvas)
  python fetch_data.py --clean  # limpa cache e refaz tudo

Requer: pip install requests pandas
"""

import os
import sys
import time
import requests
import pandas as pd
from pathlib import Path

# ── Configuração ─────────────────────────────────────────────

OUTPUT_DIR = Path("data")
TMP_DIR = OUTPUT_DIR / "tmp"
CHUNK_YEARS = 2          # janelas menores = requests menores = menos 429
DELAY_CHUNK = 5           # segundos entre chunks
DELAY_CITY = 15           # segundos entre cidades
DELAY_429_BASE = 30       # backoff base em segundos para 429
MAX_RETRIES = 5

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

START_YEAR = 2000
END_YEAR = 2024

# ── Funções ──────────────────────────────────────────────────

def city_slug(name: str) -> str:
    """Nome limpo para arquivo temporário."""
    return name.lower().replace(" ", "_").replace("ã", "a").replace("á", "a").replace("é", "e")


def build_url(lat: float, lon: float, start: str, end: str) -> str:
    return (
        f"https://archive-api.open-meteo.com/v1/archive?"
        f"latitude={lat}&longitude={lon}"
        f"&start_date={start}&end_date={end}"
        f"&daily={DAILY_VARS}"
        f"&timezone=auto"
    )


def fetch_chunk(lat: float, lon: float, start: str, end: str) -> pd.DataFrame:
    """Busca um intervalo de datas com retry e backoff para 429."""
    url = build_url(lat, lon, start, end)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(url, timeout=60)

            if resp.status_code == 429:
                wait = DELAY_429_BASE * attempt
                print(f"        429 Too Many Requests — aguardando {wait}s (tentativa {attempt}/{MAX_RETRIES})")
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
            if "429" in str(e):
                wait = DELAY_429_BASE * attempt
                print(f"        429 — aguardando {wait}s (tentativa {attempt}/{MAX_RETRIES})")
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            if attempt < MAX_RETRIES:
                wait = 10 * attempt
                print(f"        Erro: {e} — retry em {wait}s")
                time.sleep(wait)
            else:
                raise

    raise Exception(f"Falhou após {MAX_RETRIES} tentativas (429 persistente)")


def fetch_city_daily(city_info: dict) -> pd.DataFrame:
    """Busca dados diários de uma cidade em chunks pequenos."""
    name = city_info["city"]
    lat, lon = city_info["lat"], city_info["lon"]

    # Verificar cache
    tmp_file = TMP_DIR / f"{city_slug(name)}_daily.csv"
    if tmp_file.exists():
        cached = pd.read_csv(tmp_file, parse_dates=["date"])
        print(f"    ✓ Cache encontrado: {len(cached):,} dias")
        return cached

    chunks = []
    for y in range(START_YEAR, END_YEAR + 1, CHUNK_YEARS):
        y_end = min(y + CHUNK_YEARS - 1, END_YEAR)
        start_date = f"{y}-01-01"
        end_date = f"{y_end}-12-31"

        try:
            df = fetch_chunk(lat, lon, start_date, end_date)
            print(f"    ✓ {start_date} → {end_date}  ({len(df):,} dias)")
            chunks.append(df)
        except Exception as e:
            print(f"    ✗ {start_date} → {end_date}  FALHOU: {e}")

        time.sleep(DELAY_CHUNK)

    if not chunks:
        return pd.DataFrame()

    result = pd.concat(chunks, ignore_index=True)
    result["city"] = name
    result["region"] = city_info["region"]

    # Salvar cache incremental
    result.to_csv(tmp_file, index=False)
    print(f"    → Salvo em cache: {tmp_file.name} ({len(result):,} dias)")

    return result


def daily_to_monthly(df: pd.DataFrame) -> pd.DataFrame:
    """Agrega dados diários para resolução mensal."""
    df = df.copy()
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month

    monthly = df.groupby(["city", "region", "year", "month"]).agg(
        temp=("temp_mean", "mean"),
        temp_max=("temp_max", "mean"),
        temp_min=("temp_min", "mean"),
        precip=("precip", "sum"),
        wind=("wind_max", "mean"),
        radiation=("radiation", "mean"),
    ).reset_index()

    return monthly


# ── Main ─────────────────────────────────────────────────────

def main():
    # Limpar cache se --clean
    if "--clean" in sys.argv:
        import shutil
        if TMP_DIR.exists():
            shutil.rmtree(TMP_DIR)
            print("Cache limpo.\n")

    OUTPUT_DIR.mkdir(exist_ok=True)
    TMP_DIR.mkdir(exist_ok=True)

    # ── 1. São Paulo diário (Parte I) ──
    sp = next(c for c in CITIES if c["city"] == "São Paulo")
    print("=" * 55)
    print("  PARTE I — São Paulo diário")
    print("=" * 55)
    sp_daily = fetch_city_daily(sp)
    sp_daily_out = sp_daily.drop(columns=["city", "region"])
    sp_daily_out.to_csv(OUTPUT_DIR / "sp_daily.csv", index=False)
    print(f"\n→ data/sp_daily.csv  ({len(sp_daily_out):,} linhas)\n")

    time.sleep(DELAY_CITY)

    # ── 2. Todas as 12 cidades (Parte II–IV) ──
    print("=" * 55)
    print("  PARTE II–IV — 12 cidades")
    print("=" * 55)

    all_daily = []
    for i, city_info in enumerate(CITIES):
        name = city_info["city"]
        print(f"\n[{i+1:2d}/12] {name}")

        if name == "São Paulo":
            print("    (reutilizando)")
            all_daily.append(sp_daily)
            continue

        city_df = fetch_city_daily(city_info)
        if city_df.empty:
            print(f"    ⚠ SEM DADOS")
        else:
            all_daily.append(city_df)

        # Pausa maior entre cidades para evitar 429
        if i < len(CITIES) - 1:
            print(f"    ⏳ Aguardando {DELAY_CITY}s antes da próxima cidade...")
            time.sleep(DELAY_CITY)

    # Combinar e agregar
    brazil_daily = pd.concat(all_daily, ignore_index=True)
    brazil_monthly = daily_to_monthly(brazil_daily)
    brazil_monthly.to_csv(OUTPUT_DIR / "brazil_monthly.csv", index=False)

    loaded = brazil_monthly["city"].nunique()

    # ── Resumo ──
    print("\n" + "=" * 55)
    print("  RESUMO")
    print("=" * 55)
    print(f"  sp_daily.csv:        {os.path.getsize(OUTPUT_DIR / 'sp_daily.csv') / 1024:.0f} KB")
    print(f"  brazil_monthly.csv:  {os.path.getsize(OUTPUT_DIR / 'brazil_monthly.csv') / 1024:.0f} KB")
    print(f"  Cidades carregadas:  {loaded}/12")

    missing = set(c["city"] for c in CITIES) - set(brazil_monthly["city"].unique())
    if missing:
        print(f"  ⚠ Faltando: {', '.join(sorted(missing))}")
        print(f"\n  Dica: re-execute 'python fetch_data.py' — cidades já")
        print(f"  baixadas serão puladas (cache em data/tmp/).")
    else:
        print("  ✓ Todas as 12 cidades OK")
    print()


if __name__ == "__main__":
    main()
