#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FIPE Crawler - Exporta dados da Tabela FIPE para CSV usando a API pública Parallelum.

API: https://parallelum.com.br/fipe/api/v1/

Suporta tipos: carros, motos, caminhoes

Exemplos:
  python fipe_crawler.py --type carros --out fipe_carros.csv
  python fipe_crawler.py --type motos --out fipe_motos.csv --timeout 20 --retries 5
  python fipe_crawler.py --type caminhoes --out fipe_caminhoes.csv --max-brands 3 --max-models 5
"""

import argparse
import csv
import sys
import time
from typing import Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter, Retry

BASE_URL = "https://parallelum.com.br/fipe/api/v1"
VALID_TYPES = {"carros", "motos", "caminhoes"}


def build_session(timeout: int, retries: int, backoff: float) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    session.request = _timeout_request_wrapper(session.request, timeout)
    return session


def _timeout_request_wrapper(original_request, timeout: int):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return original_request(method, url, **kwargs)

    return wrapped


def get_json(session: requests.Session, url: str) -> Optional[dict]:
    try:
        resp = session.get(url)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[WARN] Falha ao requisitar {url}: {e}", file=sys.stderr)
        return None


def list_brands(session: requests.Session, vtype: str) -> List[Dict]:
    url = f"{BASE_URL}/{vtype}/marcas"
    data = get_json(session, url)
    return data or []


def list_models(session: requests.Session, vtype: str, brand_code: str) -> List[Dict]:
    url = f"{BASE_URL}/{vtype}/marcas/{brand_code}/modelos"
    data = get_json(session, url)
    if not data:
        return []
    # Resposta tem forma { modelos: [...], anos: [...] }
    modelos = data.get("modelos") or []
    return modelos


def list_years(session: requests.Session, vtype: str, brand_code: str, model_code: str) -> List[Dict]:
    url = f"{BASE_URL}/{vtype}/marcas/{brand_code}/modelos/{model_code}/anos"
    data = get_json(session, url)
    return data or []


def get_price(session: requests.Session, vtype: str, brand_code: str, model_code: str, year_code: str) -> Optional[Dict]:
    url = f"{BASE_URL}/{vtype}/marcas/{brand_code}/modelos/{model_code}/anos/{year_code}"
    data = get_json(session, url)
    return data


CSV_COLUMNS = [
    "tipo",
    "codigo_marca",
    "marca",
    "codigo_modelo",
    "modelo",
    "codigo_ano",
    "ano_modelo",
    "combustivel",
    "sigla_combustivel",
    "codigo_fipe",
    "mes_referencia",
    "valor",
]


def crawl_to_csv(
    vtype: str,
    out_path: str,
    timeout: int = 15,
    retries: int = 3,
    backoff: float = 0.5,
    rate_delay: float = 0.0,
    max_brands: Optional[int] = None,
    max_models: Optional[int] = None,
) -> None:
    assert vtype in VALID_TYPES, f"Tipo inválido: {vtype}. Use um de {sorted(VALID_TYPES)}"
    session = build_session(timeout=timeout, retries=retries, backoff=backoff)

    brands = list_brands(session, vtype)
    if max_brands is not None:
        brands = brands[:max_brands]

    total_rows = 0
    with open(out_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        for bi, brand in enumerate(brands, start=1):
            bcode = str(brand.get("codigo"))
            bname = brand.get("nome")
            print(f"[INFO] Marca {bi}/{len(brands)}: {bname} ({bcode})")
            models = list_models(session, vtype, bcode)
            if max_models is not None:
                models = models[:max_models]

            for mi, model in enumerate(models, start=1):
                mcode = str(model.get("codigo"))
                mname = model.get("nome")
                years = list_years(session, vtype, bcode, mcode)
                for yi, year in enumerate(years, start=1):
                    ycode = str(year.get("codigo"))
                    price = get_price(session, vtype, bcode, mcode, ycode)
                    if not price:
                        continue

                    row = {
                        "tipo": vtype,
                        "codigo_marca": bcode,
                        "marca": price.get("Marca") or bname,
                        "codigo_modelo": mcode,
                        "modelo": price.get("Modelo") or mname,
                        "codigo_ano": ycode,
                        "ano_modelo": price.get("AnoModelo"),
                        "combustivel": price.get("Combustivel"),
                        "sigla_combustivel": price.get("SiglaCombustivel"),
                        "codigo_fipe": price.get("CodigoFipe"),
                        "mes_referencia": price.get("MesReferencia"),
                        "valor": (price.get("Valor") or "").replace("R$", "").strip(),
                    }
                    writer.writerow(row)
                    total_rows += 1

                    if rate_delay > 0:
                        time.sleep(rate_delay)

    print(f"[DONE] CSV gerado: {out_path} com {total_rows} linhas.")


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta FIPE para CSV (Parallelum API)")
    p.add_argument(
        "--type",
        required=True,
        choices=sorted(VALID_TYPES),
        help="Tipo de veículo: carros | motos | caminhoes",
    )
    p.add_argument(
        "--out",
        required=True,
        help="Caminho do arquivo CSV de saída",
    )
    p.add_argument("--timeout", type=int, default=15, help="Timeout por requisição (s)")
    p.add_argument("--retries", type=int, default=3, help="Número de tentativas em falhas temporárias")
    p.add_argument("--backoff", type=float, default=0.5, help="Backoff exponencial entre tentativas")
    p.add_argument(
        "--rate-delay",
        type=float,
        default=0.0,
        help="Delay (s) entre requisições para evitar rate limit (ex.: 0.1)",
    )
    p.add_argument("--max-brands", type=int, default=None, help="Limita quantidade de marcas (para testes)")
    p.add_argument("--max-models", type=int, default=None, help="Limita quantidade de modelos por marca (para testes)")
    return p.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)
    try:
        crawl_to_csv(
            vtype=args.type,
            out_path=args.out,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            rate_delay=args.rate_delay,
            max_brands=args.max_brands,
            max_models=args.max_models,
        )
        return 0
    except KeyboardInterrupt:
        print("[ABORTED] Interrompido pelo usuário.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
