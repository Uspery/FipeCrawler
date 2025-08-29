from typing import Dict, List, Optional
from pathlib import Path
import json

from .http import get_json, RequestLimiter

BASE_URL = "https://fipe.parallelum.com.br/api/v2"
VALID_TYPES = {"carros", "motos", "caminhoes"}

TYPE_PATH = {
    "carros": "cars",
    "motos": "motorcycles",
    "caminhoes": "trucks",
}

# Diretório de cache (compartilhado com o projeto raiz)
CACHE_DIR = Path(".state/cache")


def list_references(session, limiter: Optional[RequestLimiter] = None) -> List[Dict]:
    url = f"{BASE_URL}/references"
    data = get_json(session, url, limiter=limiter)
    return data or []


def latest_reference_code(session) -> Optional[str]:
    refs = list_references(session)
    if not refs:
        return None
    try:
        latest = max(refs, key=lambda r: int(str(r.get("code") or 0)))
        return str(latest.get("code"))
    except Exception:
        return str(refs[0].get("code")) if refs and refs[0].get("code") is not None else None


def list_brands(session, vtype: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> List[Dict]:
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


def list_models(session, vtype: str, brand_code: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> List[Dict]:
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


def list_years(session, vtype: str, brand_code: str, model_code: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> List[Dict]:
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


def get_price(session, vtype: str, brand_code: str, model_code: str, year_code: str, reference: Optional[str], limiter: Optional[RequestLimiter] = None) -> Optional[Dict]:
    url = f"{BASE_URL}/{TYPE_PATH[vtype]}/brands/{brand_code}/models/{model_code}/years/{year_code}"
    if reference:
        url = f"{url}?reference={reference}"
    return get_json(session, url, limiter=limiter)
