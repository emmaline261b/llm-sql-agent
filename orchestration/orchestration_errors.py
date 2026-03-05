class OrchestrationError(Exception):
    """Base error for orchestration layer."""
    pass


class IntentResolutionError(OrchestrationError):
    """Intent resolver failed."""
    pass


class SQLBuildError(OrchestrationError):
    """SQL builder failed."""
    pass


class QueryExecutionError(OrchestrationError):
    """Database execution failed."""
    pass


class AnalysisError(OrchestrationError):
    """Data analyzer failed."""
    pass


class LLMError(OrchestrationError):
    """LLM call failed."""
    pass