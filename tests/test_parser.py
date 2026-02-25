from app.models import ParagraphAnalysisModel
from app.parser import build_hierarchy, normalize_ocr_lines, should_ocr_normalize


def test_hierarchy_heading_detection():
    paras = ["I. Sachverhalt", "Der Kläger trägt vor.", "1. Unterpunkt", "Text"]
    styles = ["Heading 1", "Normal", "Heading 2", "Normal"]
    out = build_hierarchy(paras, styles)
    assert out[0].is_heading is True
    assert "Sachverhalt" in out[1].hierarchy_path
    assert out[2].continuation_group != out[1].continuation_group


def test_ocr_detection_and_normalize():
    paras = ["Dies", "ist", "ein", "Abrech-", "nung", "Test"]
    assert should_ocr_normalize(paras) is True
    norm = normalize_ocr_lines(paras)
    assert "Abrechnung" in " ".join(norm)


def test_schema_validation():
    valid = {
        "keywords": ["a", "b", "c", "d", "e"],
        "issues": ["Mietvertrag"],
        "role": "LEGAL_POSITION",
        "summary_3_sentences": "Satz eins. Satz zwei. Satz drei.",
        "continuation_of_previous": False,
        "continuation_reason": None,
        "citations_norms": [],
        "citations_cases": [],
        "citations_contract": [],
        "citations_exhibits": [],
    }
    model = ParagraphAnalysisModel.model_validate(valid)
    assert model.role.value == "LEGAL_POSITION"
