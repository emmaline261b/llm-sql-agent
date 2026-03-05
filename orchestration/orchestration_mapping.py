from data_analyzer.data_schema import AnalysisSpec


def analysis_spec_from_intent(intent_res) -> AnalysisSpec:
    # TODO: docelowo mapuj po intent.entity/metric
    return AnalysisSpec(
        primary_metric="value",
        entity_label_col="fund_name",
        entity_id_col="fund_key",
        sort_direction="desc",
        language="pl",
    )