import logging

from fastapi import APIRouter

from data_analyzer.schema import AnalyzeRequest, AnalyzeResponse
from data_analyzer.analyzer import analyze
from data_analyzer.prompts import SYSTEM_PL, build_user_prompt
from llm_sql.llm.client_ollama import call_ollama_json

logger = logging.getLogger(__name__)

router = APIRouter()


@router.post("/analyze-results", response_model=AnalyzeResponse)
def analyze_results(req: AnalyzeRequest):

    logger.info("analysis.start question=%s rows=%s",
        req.question,
        req.result.row_count,
    )

    facts = analyze(req.result, req.analysis_spec)

    logger.info("analysis.facts.done metric=%s rows=%s",
        facts["metric"],
        facts["rows"],
    )

    narrative = call_ollama_json(
        model="llama3",
        system=SYSTEM_PL,
        user=build_user_prompt(
            question=req.question,
            facts=facts,
            language=req.analysis_spec.language,
        ),
    )

    logger.info("analysis.llm.done")

    return AnalyzeResponse(
        facts=facts,
        narrative=narrative,
    )