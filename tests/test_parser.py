from app.models import ParagraphAnalysisModel
from app.parser import (
    ParsedParagraph,
    build_hierarchy,
    build_semantic_blocks,
    clean_ocr_noise,
    matches_law_firm_line,
    normalize_ocr_lines,
    should_ocr_normalize,
)


def _p(idx: int, text: str, heading: bool = False, style: str = "Normal") -> ParsedParagraph:
    return ParsedParagraph(
        para_index=idx,
        text=text,
        style=style,
        is_heading=heading,
        hierarchy_path="ROOT",
        continuation_group="g1",
    )


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


def test_rubrum_detection_stops_at_first_heading_anchor():
    blocks = build_semantic_blocks([
        _p(0, "Rechtsanwaltskanzlei Muster"),
        _p(1, "Musterstraße 12, 12345 Berlin"),
        _p(2, "I. Sachverhalt", heading=True, style="Heading 1"),
        _p(3, "Der Kläger trägt vor."),
    ])
    assert blocks[0].block_type == "RUBRUM_META"
    assert "Kanzlei" in blocks[0].text_original
    assert blocks[1].block_type == "BODY"
    assert "I. Sachverhalt" in blocks[1].text_original


def test_law_firm_line_pattern_detection():
    assert matches_law_firm_line("Rechtsanwälte Beispiel")
    assert matches_law_firm_line("Musterstraße 20")
    assert matches_law_firm_line("12345 Berlin")
    assert matches_law_firm_line("Tel. 030-1234")


def test_quote_start_continue_end_and_intro_merge():
    blocks = build_semantic_blocks([
        _p(0, "I. Sachverhalt", heading=True, style="Heading 1"),
        _p(1, "Hierzu führt der BGH aus:"),
        _p(2, "„Dies ist ein langer zitierter Satz mit § 280 Abs. 1 BGB; Rn. 12."),
        _p(3, "fortgesetzt mit weiteren Gründen und Fundstellen (BGH I ZR 12/22).“"),
        _p(4, "Danach folgt normaler Fließtext."),
    ])
    quote_block = next(b for b in blocks if b.block_type == "BODY_WITH_QUOTE")
    assert quote_block.intro_text == "Hierzu führt der BGH aus:"
    assert "§ 280 Abs. 1 BGB" in quote_block.quote_text


def test_merge_short_lines_logic_and_heading_anchor_barrier():
    blocks = build_semantic_blocks([
        _p(0, "I. Antrag", heading=True, style="Heading 1"),
        _p(1, "Der Anspruch besteht weil"),
        _p(2, "und weitere Gründe folgen"),
        _p(3, "1. Unterpunkt", heading=True, style="Heading 2"),
        _p(4, "Neuer Abschnitt."),
    ])
    body_blocks = [b for b in blocks if b.block_type.startswith("BODY")]
    assert "weitere Gründe" in body_blocks[0].text_original
    assert body_blocks[1].text_original.startswith("1. Unterpunkt")


def test_clean_ocr_noise_removes_page_numbers_and_repeats_but_keeps_accounts():
    paras = [
        "Seite 1 von 3",
        "123",
        "Kostenstelle 4400",
        "1000 - Mieterlöse",
    ] + ["ACME GmbH 2024"] * 8 + ["Sachverhalt beginnt"]
    styles = ["Normal"] * len(paras)

    result = clean_ocr_noise(paras, styles, repeat_threshold=8)

    assert any(r.reason == "PAGE_NUMBER_RULE" for r in result.removed_lines)
    assert any(r.reason == "HEADER_FOOTER_CANDIDATE" for r in result.removed_lines)
    assert "Kostenstelle 4400" in result.kept_paragraphs
    assert "1000 - Mieterlöse" in result.kept_paragraphs
    assert result.kept_account_headings_count >= 2


def test_clean_ocr_noise_ambiguous_defaults_keep():
    paras = ["123", "Konto 1200", "123"]
    styles = ["Normal", "Normal", "Normal"]

    result = clean_ocr_noise(
        paras,
        styles,
        repeat_threshold=2,
        classify_ambiguous=lambda _line, _prev, _next: ("KEEP", "unsicher"),
    )

    assert "Konto 1200" in result.kept_paragraphs
