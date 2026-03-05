import logging

from fastapi import APIRouter, HTTPException

from orchestration.orchestration_service import handle_ask
from orchestration.orchestration_errors import (
    IntentResolutionError,
    SQLBuildError,
    QueryExecutionError,
    AnalysisError,
    LLMError,
)
from orchestration.orchestration_models import AskRequest, AskResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["execution"])


@router.post("/ask", response_model=AskResponse)
def ask(req: AskRequest):
    try:
        return handle_ask(req)

    except IntentResolutionError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except SQLBuildError as e:
        raise HTTPException(status_code=400, detail=str(e))

    except QueryExecutionError as e:
        raise HTTPException(status_code=500, detail=str(e))

    except AnalysisError as e:
        raise HTTPException(status_code=500, detail=str(e))

    except LLMError as e:
        raise HTTPException(status_code=502, detail=str(e))
