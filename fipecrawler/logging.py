from datetime import datetime


def ts() -> str:
    return datetime.now().isoformat(timespec="seconds")


def log_start(context: str, **fields):
    kv = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"[START] {context} | time={ts()} | {kv}")


def log_state(**fields):
    kv = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"[STATE] {kv}")


def log_stage(**fields):
    kv = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"[STAGE] {kv}")


def log_resume(**fields):
    kv = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"[RESUME] {kv}")


def log_stats(**fields):
    kv = " ".join(f"{k}={v}" for k, v in fields.items())
    print(f"[STATS] {kv}")


def log_ref(reference):
    print(f"[REF] using latest reference={reference}")


def log_cont(msg: str):
    print(f"[CONT] {msg}")


def log_next(msg: str):
    print(f"[NEXT] {msg}")
