import os
import time
from pathlib import Path
from typing import Optional

from .http import RequestLimiter, build_session
from .api import TYPE_PATH, list_brands, list_models, list_years, get_price
from .state import today_key, load_state, save_state, clear_state, append_csv_row
from .logging import log_start, log_state, log_resume, log_stage, log_ref


class FullScanner:
    def __init__(
        self,
        *,
        timeout: int,
        retries: int,
        backoff: float,
        rate_delay: float,
        token: Optional[str],
        reference: Optional[str],
    ) -> None:
        self.timeout = timeout
        self.retries = retries
        self.backoff = backoff
        self.rate_delay = rate_delay
        self.token = token
        self.reference = reference

    def run(self) -> int:
        # Config via .env
        full_dir = Path(os.getenv("FULL_SCAN_DIR", "full_scan"))
        limit = int(os.getenv("DAILY_LIMIT", "500"))
        margin = int(os.getenv("LIMIT_MARGIN", "10"))
        if margin >= limit:
            print(f"[WARN] LIMIT_MARGIN (={margin}) >= DAILY_LIMIT (={limit}); ajustando margem para {max(0, limit-1)}")
            margin = max(0, limit - 1)

        state = load_state()
        today = today_key()
        used = int(state.get("used", 0))
        if state.get("date") != today:
            used = 0
        limiter = RequestLimiter(limit=limit, margin=margin, used=used, date_key=today)

        types_order = ["carros", "motos", "caminhoes"]
        type_idx = int(state.get("type_index", 0))
        brand_idx = int(state.get("brand_index", 0))
        model_idx = int(state.get("model_index", 0))
        year_idx = int(state.get("year_index", 0))

        cur_ti, cur_bi, cur_mi, cur_yi = type_idx, brand_idx, model_idx, year_idx

        session = build_session(timeout=self.timeout, retries=self.retries, backoff=self.backoff, token=self.token)

        if not self.reference or (isinstance(self.reference, str) and self.reference.strip().lower() == "latest"):
            # Resolve referência em quem chamar (CLI) para evitar uma requisição extra aqui, se já resolvida
            from .api import latest_reference_code

            self.reference = latest_reference_code(session)
            log_ref(self.reference)

        log_start(
            "Full scan",
            ref=self.reference or "-",
            full_dir=full_dir,
            limit=limit,
            margin=margin,
            date=today,
        )
        log_state(
            checkpoint=str(Path(".state") / "full_scan.json"),
            loaded=f"type={type_idx} brand={brand_idx} model={model_idx} year={year_idx} used={used}/{limit}",
        )

        processed_rows = 0
        try:
            log_resume(
                ref=self.reference or "-",
                type_idx=type_idx,
                brand_idx=brand_idx,
                model_idx=model_idx,
                year_idx=year_idx,
                used=f"{used}/{limit}",
            )
            for ti in range(type_idx, len(types_order)):
                cur_ti = ti
                vtype = types_order[ti]
                brands = list_brands(session, vtype, self.reference, limiter=limiter)
                log_stage(type=vtype, brands=len(brands), start_brand_index=(brand_idx if ti == type_idx else 0))
                for bi in range(brand_idx, len(brands)):
                    cur_bi = bi
                    b = brands[bi]
                    bcode, bname = str(b.get("code")), b.get("name")
                    if bi == brand_idx and model_idx > 0:
                        print(f"[CONT] {vtype} brand={bname}({bcode}) model_idx={model_idx} year_idx={year_idx}")
                    else:
                        print(f"[CONT] {vtype} brand={bname}({bcode}) model_idx=0 year_idx=0")
                    models = list_models(session, vtype, bcode, self.reference, limiter=limiter)
                    log_stage(
                        brand=f"{bname}({bcode})",
                        models=len(models),
                        start_model_index=(model_idx if (ti == type_idx and bi == brand_idx) else 0),
                    )
                    for mi in range(model_idx, len(models)):
                        cur_mi = mi
                        m = models[mi]
                        mcode, mname = str(m.get("code")), m.get("name")
                        years = list_years(session, vtype, bcode, mcode, self.reference, limiter=limiter)
                        log_stage(
                            model=f"{mname}({mcode})",
                            years=len(years),
                            start_year_index=(
                                year_idx if (ti == type_idx and bi == brand_idx and mi == model_idx) else 0
                            ),
                        )
                        for yi in range(year_idx, len(years)):
                            cur_yi = yi
                            y = years[yi]
                            ycode = str(y.get("code"))
                            if yi == year_idx:
                                print(f"[NEXT] model={mname}({mcode}) start_year_pos={year_idx}/{len(years)}")
                            if self.rate_delay > 0:
                                time.sleep(self.rate_delay)
                            price = get_price(
                                session, vtype, bcode, mcode, ycode, self.reference, limiter=limiter
                            )
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
                            append_csv_row(out_csv, row, headers=[
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
                            ])
                            processed_rows += 1

                            state.update(
                                {
                                    "date": today,
                                    "used": limiter.used,
                                    "type_index": ti,
                                    "brand_index": bi,
                                    "model_index": mi,
                                    "year_index": yi + 1,
                                    "reference": self.reference,
                                    "out_dir": str(full_dir),
                                }
                            )
                            save_state(state)

                        year_idx = 0
                        state.update({"year_index": 0, "model_index": mi + 1})
                        save_state(state)
                    model_idx = 0
                    state.update({"model_index": 0, "brand_index": bi + 1})
                    save_state(state)
                brand_idx = 0
                state.update({"brand_index": 0, "type_index": ti + 1})
                save_state(state)

            clear_state()
            print(f"[DONE] Full scan concluído. Arquivos em: {full_dir}")
            print(f"[STATS] processed_rows={processed_rows} used_today={limiter.used}/{limiter.limit}")
            return 0
        except RuntimeError as e:
            state.update(
                {
                    "date": today,
                    "used": limiter.used,
                    "type_index": cur_ti,
                    "brand_index": cur_bi,
                    "model_index": cur_mi,
                    "year_index": cur_yi,
                    "reference": self.reference,
                    "out_dir": str(full_dir),
                }
            )
            save_state(state)
            print(f"[PAUSED] {e} | Usadas: {limiter.used}/{limiter.limit}. Retome amanhã.")
            print(f"[STATS] processed_rows={processed_rows} used_today={limiter.used}/{limiter.limit}")
            return 0
