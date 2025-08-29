import concurrent.futures as futures
import time
from typing import Dict, Iterable, List, Optional

from .http import build_session, get_thread_session
from .api import VALID_TYPES, TYPE_PATH, latest_reference_code, list_brands, list_models, list_years, get_price
from .logging import log_start, log_stage, log_ref

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


class Exporter:
    def __init__(
        self,
        vtype: str,
        out_path: str,
        *,
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
        self.vtype = vtype
        self.out_path = out_path
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.rate_delay = rate_delay
        self.max_brands = max_brands
        self.max_models = max_models
        self.workers = workers
        self.token = token
        self.reference = reference

    def run(self) -> int:
        import csv

        assert self.vtype in VALID_TYPES, f"Tipo inválido: {self.vtype}. Use um de {sorted(VALID_TYPES)}"
        session = build_session(timeout=self.timeout, retries=self.retries, backoff=self.backoff, token=self.token)
        # Resolve referência
        if not self.reference or (isinstance(self.reference, str) and self.reference.strip().lower() == "latest"):
            self.reference = latest_reference_code(session)
            log_ref(self.reference)

        log_start(
            "Export",
            type=self.vtype,
            out=self.out_path,
            ref=self.reference,
            workers=self.workers,
            rate_delay=self.rate_delay,
        )

        brands = list_brands(session, self.vtype, self.reference)
        if self.max_brands is not None:
            brands = brands[: self.max_brands]
        log_stage(type=self.vtype, brands=len(brands))

        total_rows = 0
        with open(self.out_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            writer.writeheader()

            with futures.ThreadPoolExecutor(max_workers=max(1, int(self.workers))) as executor:
                for bi, brand in enumerate(brands, start=1):
                    bcode = str(brand.get("code"))
                    bname = brand.get("name")
                    print(f"[INFO] Marca {bi}/{len(brands)}: {bname} ({bcode})")
                    models = list_models(session, self.vtype, bcode, self.reference)
                    if self.max_models is not None:
                        models = models[: self.max_models]
                    log_stage(brand=f"{bname}({bcode})", models=len(models))

                    for mi, model in enumerate(models, start=1):
                        mcode = str(model.get("code"))
                        mname = model.get("name")
                        years = list_years(session, self.vtype, bcode, mcode, self.reference)
                        log_stage(model=f"{mname}({mcode})", years=len(years))

                        future_to_year = [
                            executor.submit(
                                _fetch_row,
                                self.vtype,
                                bcode,
                                bname,
                                mcode,
                                mname,
                                str(year.get("code")),
                                self.timeout,
                                self.retries,
                                self.backoff,
                                self.rate_delay,
                                self.token,
                                self.reference,
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
                                print(f"[WARN] Falha ao processar ano: {e}")

        print(f"[DONE] CSV gerado: {self.out_path} com {total_rows} linhas.")
        return 0
