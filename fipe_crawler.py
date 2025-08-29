#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
FIPE Crawler - CLI

Uso:
  python fipe_crawler.py --type carros --out fipe_carros.csv
  python fipe_crawler.py --full-scan
  python fipe_crawler.py --list-references
"""

import argparse
import os
import sys
from typing import Iterable, Optional
from datetime import datetime

from dotenv import load_dotenv
from fipecrawler import api as api_mod
from fipecrawler import http as http_mod
from fipecrawler import export as export_mod
from fipecrawler import fullscan as fullscan_mod
from fipecrawler import VALID_TYPES






def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Exporta FIPE para CSV (Parallelum API)")
    p.add_argument(
        "--type",
        required=False,
        choices=sorted(VALID_TYPES),
        help="Tipo de veículo: carros | motos | caminhoes",
    )
    p.add_argument(
        "--out",
        required=False,
        help="Caminho do arquivo CSV de saída",
    )
    p.add_argument("--timeout", type=int, default=None, help="Timeout por requisição (s)")
    p.add_argument("--retries", type=int, default=None, help="Número de tentativas em falhas temporárias")
    p.add_argument("--backoff", type=float, default=None, help="Backoff exponencial entre tentativas")
    p.add_argument(
        "--rate-delay",
        type=float,
        default=None,
        help="Delay (s) entre requisições para evitar rate limit (ex.: 0.1)",
    )
    p.add_argument("--max-brands", type=int, default=None, help="Limita quantidade de marcas (para testes)")
    p.add_argument("--max-models", type=int, default=None, help="Limita quantidade de modelos por marca (para testes)")
    p.add_argument("--workers", type=int, default=None, help="Número de requisições concorrentes")
    p.add_argument("--token", type=str, default=None, help="X-Subscription-Token para a API v2 (limite p/ dia)")
    p.add_argument("--reference", type=str, default=None, help="Código de referência do mês (ex.: 308)")
    p.add_argument("--list-references", action="store_true", help="Apenas lista os códigos/meses de referência e sai")
    p.add_argument("--full-scan", action="store_true", help="Varre carros->motos->caminhoes respeitando o limite diário e cria CSVs em full_scan/")
    return p.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    load_dotenv()
    args = parse_args(argv)

    # Helpers para pegar env com cast
    def _env_int(key: str, default: Optional[int]) -> Optional[int]:
        v = os.getenv(key)
        if v is None:
            return default
        try:
            return int(v)
        except ValueError:
            return default

    def _env_float(key: str, default: Optional[float]) -> Optional[float]:
        v = os.getenv(key)
        if v is None:
            return default
        try:
            return float(v)
        except ValueError:
            return default

    def _env_str(key: str, default: Optional[str]) -> Optional[str]:
        v = os.getenv(key)
        return v if v is not None else default

    # Merge com .env (CLI tem precedência)
    args.type = args.type or _env_str("TYPE", None)
    args.out = args.out or _env_str("OUT", None)
    args.token = args.token or _env_str("TOKEN", None)
    args.reference = args.reference or _env_str("REFERENCE", None)
    args.timeout = args.timeout if args.timeout is not None else _env_int("TIMEOUT", 15)
    args.retries = args.retries if args.retries is not None else _env_int("RETRIES", 3)
    args.backoff = args.backoff if args.backoff is not None else _env_float("BACKOFF", 0.5)
    args.rate_delay = args.rate_delay if args.rate_delay is not None else _env_float("RATE_DELAY", 0.0)
    args.max_brands = args.max_brands if args.max_brands is not None else _env_int("MAX_BRANDS", None)
    args.max_models = args.max_models if args.max_models is not None else _env_int("MAX_MODELS", None)
    args.workers = args.workers if args.workers is not None else _env_int("WORKERS", 1)

    try:
        print(
            f"[START] {('Full Scan' if args.full_scan else 'Export')} | time={datetime.now().isoformat(timespec='seconds')} | type={args.type or '-'} | out={args.out or '-'} | ref={args.reference or 'latest'}"
        )
        if args.list_references:
            sess = http_mod.build_session(timeout=args.timeout, retries=args.retries, backoff=args.backoff, token=args.token)
            refs = api_mod.list_references(sess)
            if not refs:
                print("[INFO] Nenhuma referência retornada (verifique o TOKEN)")
                return 0
            print("code,month")
            for r in refs:
                print(f"{r.get('code')},{r.get('month')}")
            return 0

        if args.full_scan:
            ref = args.reference
            if not ref or (isinstance(ref, str) and ref.strip().lower() == "latest"):
                sess = http_mod.build_session(timeout=args.timeout, retries=args.retries, backoff=args.backoff, token=args.token)
                ref = api_mod.latest_reference_code(sess)
                print(f"[REF] using latest reference={ref}")
            scanner = fullscan_mod.FullScanner(
                timeout=args.timeout,
                retries=args.retries,
                backoff=args.backoff,
                rate_delay=args.rate_delay,
                token=args.token,
                reference=ref,
            )
            return scanner.run()

        if not args.type or not args.out:
            print("usage: --type {caminhoes,carros,motos} --out OUT [opções]", file=sys.stderr)
            return 2

        exporter = export_mod.Exporter(
            vtype=args.type,
            out_path=args.out,
            timeout=args.timeout,
            retries=args.retries,
            backoff=args.backoff,
            rate_delay=args.rate_delay,
            max_brands=args.max_brands,
            max_models=args.max_models,
            workers=args.workers,
            token=args.token,
            reference=(
                args.reference
                if args.reference and str(args.reference).strip().lower() != "latest"
                else (
                    lambda _s: (print(f"[REF] using latest reference={api_mod.latest_reference_code(_s)}") or api_mod.latest_reference_code(_s))
                )(http_mod.build_session(timeout=args.timeout, retries=args.retries, backoff=args.backoff, token=args.token))
            ),
        )
        exporter.run()
        return 0
    except KeyboardInterrupt:
        print("[ABORTED] Interrompido pelo usuário.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1



if __name__ == "__main__":
    raise SystemExit(main())
