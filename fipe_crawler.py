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
import threading
import os
import concurrent.futures as futures
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from requests.adapters import HTTPAdapter, Retry
from dotenv import load_dotenv
import json
from datetime import datetime
from pathlib import Path

BASE_URL = "https://fipe.parallelum.com.br/api/v2"
VALID_TYPES = {"carros", "motos", "caminhoes"}

# Diretórios de estado/cache
CACHE_DIR = Path(".state/cache")

# Mapeia tipos em PT-BR para os paths da API v2
TYPE_PATH = {
    "carros": "cars",
    "motos": "motorcycles",
    "caminhoes": "trucks",
}

# Thread-local para manter uma Session por thread
_thread_local = threading.local()


# ---------------- Limiter & Checkpoint (full scan) -----------------
class RequestLimiter:
    def __init__(self, limit: int, margin: int, used: int = 0, date_key: Optional[str] = None):
        self.limit = max(0, int(limit))
        self.margin = max(0, int(margin))
        self.used = max(0, int(used))
        self.date_key = date_key  # e.g., YYYY-MM-DD

    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def can_make_request(self) -> bool:
        # stop when remaining <= margin
        return self.remaining() > self.margin

    def on_request(self):
        self.used += 1


def build_session(timeout: int, retries: int, backoff: float, token: Optional[str] = None) -> requests.Session:
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
    if token:
        session.headers.update({"X-Subscription-Token": token, "accept": "application/json"})
    return session


def get_thread_session(timeout: int, retries: int, backoff: float, token: Optional[str]) -> requests.Session:
    """Retorna uma Session específica da thread, criando se necessário."""
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = build_session(timeout=timeout, retries=retries, backoff=backoff, token=token)
        _thread_local.session = sess
    return sess


def _timeout_request_wrapper(original_request, timeout: int):
    def wrapped(method, url, **kwargs):
        if "timeout" not in kwargs:
            kwargs["timeout"] = timeout
        return original_request(method, url, **kwargs)

    return wrapped


def get_json(session: requests.Session, url: str, limiter: Optional[RequestLimiter] = None) -> Optional[dict]:
    try:
        if limiter is not None:
            if not limiter.can_make_request():
                raise RuntimeError("Daily request limit margin reached; stopping to resume later.")
            limiter.on_request()
        resp = session.get(url)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"[WARN] Falha ao requisitar {url}: {e}", file=sys.stderr)
        return None


def list_references(session: requests.Session, limiter: Optional[RequestLimiter] = None) -> List[Dict]:
    """Lista códigos/meses de referência da FIPE v2."""
    url = f"{BASE_URL}/references"
    data = get_json(session, url, limiter=limiter)
    return data or []


def latest_reference_code(session: requests.Session) -> Optional[str]:
    """Obtém o código da referência mais recente (maior código)."""
    refs = list_references(session)
    if not refs:
        return None
    try:
        latest = max(refs, key=lambda r: int(str(r.get("code") or 0)))
        return str(latest.get("code"))
    except Exception:
        # fallback: primeiro elemento
        return str(refs[0].get("code")) if refs and refs[0].get("code") is not None else None


def list_brands(session: requests.Session, vtype: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> List[Dict]:
    # Cache somente quando há reference resolvida
    if reference:
        path = CACHE_DIR / str(reference) / TYPE_PATH[vtype] / "brands.json"
        try:
            if path.exists():
                print(f"[CACHE] brands type={TYPE_PATH[vtype]} ref={reference}")
                return json.loads(path.read_text(encoding="utf-8")) or []
        except Exception:
            pass
    url = f"{BASE_URL}/{TYPE_PATH[vtype]}/brands"
    if reference:
        url = f"{url}?reference={reference}"
    data = get_json(session, url, limiter=limiter) or []
    if reference:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            print(f"[HTTP] brands type={TYPE_PATH[vtype]} ref={reference} (cached)")
        except Exception:
            pass
    return data


def list_models(session: requests.Session, vtype: str, brand_code: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> List[Dict]:
    if reference:
        path = CACHE_DIR / str(reference) / TYPE_PATH[vtype] / f"models_{brand_code}.json"
        try:
            if path.exists():
                print(f"[CACHE] models type={TYPE_PATH[vtype]} brand={brand_code} ref={reference}")
                return json.loads(path.read_text(encoding="utf-8")) or []
        except Exception:
            pass
    url = f"{BASE_URL}/{TYPE_PATH[vtype]}/brands/{brand_code}/models"
    if reference:
        url = f"{url}?reference={reference}"
    data = get_json(session, url, limiter=limiter) or []
    if reference:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            print(f"[HTTP] models type={TYPE_PATH[vtype]} brand={brand_code} ref={reference} (cached)")
        except Exception:
            pass
    return data


def list_years(session: requests.Session, vtype: str, brand_code: str, model_code: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> List[Dict]:
    if reference:
        path = CACHE_DIR / str(reference) / TYPE_PATH[vtype] / f"years_{brand_code}_{model_code}.json"
        try:
            if path.exists():
                print(f"[CACHE] years type={TYPE_PATH[vtype]} brand={brand_code} model={model_code} ref={reference}")
                return json.loads(path.read_text(encoding="utf-8")) or []
        except Exception:
            pass
    url = f"{BASE_URL}/{TYPE_PATH[vtype]}/brands/{brand_code}/models/{model_code}/years"
    if reference:
        url = f"{url}?reference={reference}"
    data = get_json(session, url, limiter=limiter) or []
    if reference:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            print(f"[HTTP] years type={TYPE_PATH[vtype]} brand={brand_code} model={model_code} ref={reference} (cached)")
        except Exception:
            pass
    return data


def get_price(session: requests.Session, vtype: str, brand_code: str, model_code: str, year_code: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> Optional[Dict]:
    url = f"{BASE_URL}/{TYPE_PATH[vtype]}/brands/{brand_code}/models/{model_code}/years/{year_code}"
    if reference:
        url = f"{url}?reference={reference}"
    data = get_json(session, url, limiter=limiter)
    return data


def _fetch_row(
    vtype: str,
    bcode: str,
    bname: str,
    mcode: str,
    mname: str,
    ycode: str,
    timeout: int,
    retries: int,
    backoff: float,
    rate_delay: float,
    token: Optional[str],
    reference: Optional[str],
) -> Optional[Dict]:
    if rate_delay > 0:
        time.sleep(rate_delay)
    session = get_thread_session(timeout=timeout, retries=retries, backoff=backoff, token=token)
    price = get_price(session, vtype, bcode, mcode, ycode, reference)
    if not price:
        return None
    return {
        "tipo": vtype,
        "codigo_marca": bcode,
        "marca": price.get("brand") or bname,
        "codigo_modelo": mcode,
        "modelo": price.get("model") or mname,
        "codigo_ano": ycode,
        "ano_modelo": price.get("modelYear"),
        "combustivel": price.get("fuel"),
        "sigla_combustivel": price.get("fuelAcronym"),
        "codigo_fipe": price.get("codeFipe"),
        "mes_referencia": price.get("referenceMonth"),
        "valor": (price.get("price") or "").replace("R$", "").strip(),
    }


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
    workers: int = 1,
    token: Optional[str] = None,
    reference: Optional[str] = None,
) -> None:
    assert vtype in VALID_TYPES, f"Tipo inválido: {vtype}. Use um de {sorted(VALID_TYPES)}"
    session = build_session(timeout=timeout, retries=retries, backoff=backoff, token=token)
    # Se referência não vier definida, usar a mais recente
    if not reference or (isinstance(reference, str) and reference.strip().lower() == "latest"):
        reference = latest_reference_code(session)
        print(f"[REF] using latest reference={reference}")

    print(
        f"[START] Export | time={datetime.now().isoformat(timespec='seconds')} | type={vtype} | out={out_path} | ref={reference} | workers={workers} | rate_delay={rate_delay}"
    )

    brands = list_brands(session, vtype, reference)
    if max_brands is not None:
        brands = brands[:max_brands]
    print(f"[STAGE] type={vtype} brands={len(brands)}")

    total_rows = 0
    with open(out_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        writer.writeheader()

        with futures.ThreadPoolExecutor(max_workers=max(1, int(workers))) as executor:
            for bi, brand in enumerate(brands, start=1):
                bcode = str(brand.get("code"))
                bname = brand.get("name")
                print(f"[INFO] Marca {bi}/{len(brands)}: {bname} ({bcode})")
                models = list_models(session, vtype, bcode, reference)
                if max_models is not None:
                    models = models[:max_models]
                print(f"[STAGE] brand={bname}({bcode}) models={len(models)}")

                for mi, model in enumerate(models, start=1):
                    mcode = str(model.get("code"))
                    mname = model.get("name")
                    years = list_years(session, vtype, bcode, mcode, reference)
                    print(f"[STAGE] model={mname}({mcode}) years={len(years)}")

                    future_to_year = [
                        executor.submit(
                            _fetch_row,
                            vtype,
                            bcode,
                            bname,
                            mcode,
                            mname,
                            str(year.get("code")),
                            timeout,
                            retries,
                            backoff,
                            rate_delay,
                            token,
                            reference,
                        )
                        for year in years
                    ]

                    for fut in futures.as_completed(future_to_year):
                        try:
                            row = fut.result()
                            if row:
                                writer.writerow(row)
                                total_rows += 1
                        except Exception as e:
                            print(f"[WARN] Falha ao processar ano: {e}", file=sys.stderr)

    print(f"[DONE] CSV gerado: {out_path} com {total_rows} linhas.")


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
    # Carrega .env (se existir) para permitir TOKEN e REFERENCE por ambiente
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

    # Preenchimento a partir do .env (CLI tem precedência)
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
        # Modo de listagem de referências
        if args.list_references:
            sess = build_session(timeout=args.timeout, retries=args.retries, backoff=args.backoff, token=args.token)
            refs = list_references(sess)
            if not refs:
                print("[INFO] Nenhuma referência retornada (verifique o TOKEN)")
                return 0
            # Imprime como CSV simples no stdout
            print("code,month")
            for r in refs:
                print(f"{r.get('code')},{r.get('month')}")
            return 0
        # Modo full scan
        if args.full_scan:
            # Referência: usar mais recente se não informada ou se 'latest'
            ref = args.reference
            if not ref or (isinstance(ref, str) and ref.strip().lower() == "latest"):
                sess = build_session(timeout=args.timeout, retries=args.retries, backoff=args.backoff, token=args.token)
                ref = latest_reference_code(sess)
                print(f"[REF] using latest reference={ref}")
            return run_full_scan(
                timeout=args.timeout,
                retries=args.retries,
                backoff=args.backoff,
                rate_delay=args.rate_delay,
                token=args.token,
                reference=ref,
            )
        # Validação para modo de exportação
        if not args.type or not args.out:
            print("usage: --type {caminhoes,carros,motos} --out OUT [opções]", file=sys.stderr)
            return 2
        crawl_to_csv(
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
                    lambda _s: (print(f"[REF] using latest reference={latest_reference_code(_s)}") or latest_reference_code(_s))
                )(build_session(timeout=args.timeout, retries=args.retries, backoff=args.backoff, token=args.token))
            ),
        )
        return 0
    except KeyboardInterrupt:
        print("[ABORTED] Interrompido pelo usuário.", file=sys.stderr)
        return 130
    except Exception as e:
        print(f"[ERROR] {e}", file=sys.stderr)
        return 1



# ---------------- Full Scan Implementation -----------------


STATE_DIR = Path(".state")
STATE_FILE = STATE_DIR / "full_scan.json"


def _today_key() -> str:
    return datetime.now().strftime("%Y-%m-%d")


def _load_state() -> Dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_state(state: Dict) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, ensure_ascii=False, indent=2), encoding="utf-8")


def _clear_state() -> None:
    try:
        if STATE_FILE.exists():
            STATE_FILE.unlink()
    except Exception:
        pass


def _append_csv_row(csv_path: Path, row: Dict) -> None:
    new_file = not csv_path.exists()
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)


def run_full_scan(
    timeout: int,
    retries: int,
    backoff: float,
    rate_delay: float,
    token: Optional[str],
    reference: Optional[str],
) -> int:
    # Config via .env
    full_dir = Path(os.getenv("FULL_SCAN_DIR", "full_scan"))
    limit = int(os.getenv("DAILY_LIMIT", "500"))
    margin = int(os.getenv("LIMIT_MARGIN", "10"))
    # Evita pausa imediata por configuração incorreta
    if margin >= limit:
        print(f"[WARN] LIMIT_MARGIN (={margin}) >= DAILY_LIMIT (={limit}); ajustando margem para {max(0, limit-1)}")
        margin = max(0, limit - 1)

    state = _load_state()
    today = _today_key()
    used = int(state.get("used", 0))
    if state.get("date") != today:
        # Reset contagem diária ao virar o dia
        used = 0
    limiter = RequestLimiter(limit=limit, margin=margin, used=used, date_key=today)

    types_order = ["carros", "motos", "caminhoes"]
    type_idx = int(state.get("type_index", 0))
    brand_idx = int(state.get("brand_index", 0))
    model_idx = int(state.get("model_index", 0))
    year_idx = int(state.get("year_index", 0))

    # Índices correntes (para salvar exatamente onde parou, mesmo em exceção)
    cur_ti, cur_bi, cur_mi, cur_yi = type_idx, brand_idx, model_idx, year_idx

    session = build_session(timeout=timeout, retries=retries, backoff=backoff, token=token)
    print(
        f"[START] Full scan | time={datetime.now().isoformat(timespec='seconds')} | ref={reference or '-'} | full_dir={full_dir} | limit={limit} margin={margin} | date={today}"
    )
    print(
        f"[STATE] checkpoint={STATE_FILE} | loaded: type={type_idx} brand={brand_idx} model={model_idx} year={year_idx} used={used}/{limit}"
    )
    
    processed_rows = 0
    try:
        # Log de retomada
        print(
            f"[RESUME] ref={reference or '-'} type_idx={type_idx} brand_idx={brand_idx} model_idx={model_idx} year_idx={year_idx} used={used}/{limit}"
        )
        for ti in range(type_idx, len(types_order)):
            cur_ti = ti
            vtype = types_order[ti]
            brands = list_brands(session, vtype, reference, limiter=limiter)
            print(
                f"[STAGE] type={vtype} brands={len(brands)} start_brand_index={(brand_idx if ti == type_idx else 0)}"
            )
            for bi in range(brand_idx, len(brands)):
                cur_bi = bi
                b = brands[bi]
                bcode, bname = str(b.get("code")), b.get("name")
                if bi == brand_idx and model_idx > 0:
                    print(f"[CONT] {vtype} brand={bname}({bcode}) model_idx={model_idx} year_idx={year_idx}")
                else:
                    print(f"[CONT] {vtype} brand={bname}({bcode}) model_idx=0 year_idx=0")
                models = list_models(session, vtype, bcode, reference, limiter=limiter)
                print(
                    f"[STAGE] brand={bname}({bcode}) models={len(models)} start_model_index={(model_idx if (ti == type_idx and bi == brand_idx) else 0)}"
                )
                for mi in range(model_idx, len(models)):
                    cur_mi = mi
                    m = models[mi]
                    mcode, mname = str(m.get("code")), m.get("name")
                    years = list_years(session, vtype, bcode, mcode, reference, limiter=limiter)
                    print(
                        f"[STAGE] model={mname}({mcode}) years={len(years)} start_year_index={(year_idx if (ti == type_idx and bi == brand_idx and mi == model_idx) else 0)}"
                    )
                    for yi in range(year_idx, len(years)):
                        cur_yi = yi
                        y = years[yi]
                        ycode = str(y.get("code"))
                        if yi == year_idx:
                            print(f"[NEXT] model={mname}({mcode}) start_year_pos={year_idx}/{len(years)}")
                        if rate_delay > 0:
                            time.sleep(rate_delay)
                        price = get_price(session, vtype, bcode, mcode, ycode, reference, limiter=limiter)
                        if not price:
                            continue
                        row = {
                            "tipo": vtype,
                            "codigo_marca": bcode,
                            "marca": price.get("brand") or bname,
                            "codigo_modelo": mcode,
                            "modelo": price.get("model") or mname,
                            "codigo_ano": ycode,
                            "ano_modelo": price.get("modelYear"),
                            "combustivel": price.get("fuel"),
                            "sigla_combustivel": price.get("fuelAcronym"),
                            "codigo_fipe": price.get("codeFipe"),
                            "mes_referencia": price.get("referenceMonth"),
                            "valor": (price.get("price") or "").replace("R$", "").strip(),
                        }
                        out_csv = full_dir / f"{TYPE_PATH[vtype]}.csv"
                        _append_csv_row(out_csv, row)
                        processed_rows += 1

                        # Atualiza índices após cada ano
                        state.update(
                            {
                                "date": today,
                                "used": limiter.used,
                                "type_index": ti,
                                "brand_index": bi,
                                "model_index": mi,
                                "year_index": yi + 1,
                                "reference": reference,
                                "out_dir": str(full_dir),
                            }
                        )
                        _save_state(state)

                    # Reset year idx e avança model
                    year_idx = 0
                    state.update({"year_index": 0, "model_index": mi + 1})
                    _save_state(state)
                # Reset model idx e avança brand
                model_idx = 0
                state.update({"model_index": 0, "brand_index": bi + 1})
                _save_state(state)
            # Reset brand idx e avança type
            brand_idx = 0
            state.update({"brand_index": 0, "type_index": ti + 1})
            _save_state(state)

        # Finalizado tudo: limpar estado
        _clear_state()
        print(f"[DONE] Full scan concluído. Arquivos em: {full_dir}")
        print(f"[STATS] processed_rows={processed_rows} used_today={limiter.used}/{limiter.limit}")
        return 0
    except RuntimeError as e:
        # Provavelmente atingiu a margem do limite
        state.update(
            {
                "date": today,
                "used": limiter.used,
                "type_index": cur_ti,
                "brand_index": cur_bi,
                "model_index": cur_mi,
                # Retomar exatamente neste ano (não incrementa, pois não processou)
                "year_index": cur_yi,
                "reference": reference,
                "out_dir": str(full_dir),
            }
        )
        _save_state(state)
        print(f"[PAUSED] {e} | Usadas: {limiter.used}/{limiter.limit}. Retome amanhã.")
        print(f"[STATS] processed_rows={processed_rows} used_today={limiter.used}/{limiter.limit}")
        return 0


if __name__ == "__main__":
    raise SystemExit(main())
