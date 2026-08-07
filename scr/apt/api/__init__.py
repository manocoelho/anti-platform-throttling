"""API REST (FastAPI) e, no mesmo processo, o scheduler.

O dispatcher roda como background task iniciada no lifespan de `main.py` em vez
de container separado -- decisao e trade-offs em ADR-010.

    main.py      aplicacao, lifespan, middleware de correlacao
    deps.py      dependencias injetaveis e AppState
    schemas.py   contrato HTTP (validacao + OpenAPI)
    routers/     um modulo por area
"""

from apt.api.main import app, create_app

__all__ = ["app", "create_app"]
