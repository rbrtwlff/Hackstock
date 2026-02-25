from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field, field_validator


class RoleEnum(str, Enum):
    FACT_ASSERTION = "FACT_ASSERTION"
    FACT_DENIAL = "FACT_DENIAL"
    LEGAL_POSITION = "LEGAL_POSITION"
    SUBSUMPTION = "SUBSUMPTION"
    CONTRACT_INTERPRETATION = "CONTRACT_INTERPRETATION"
    CALCULATION = "CALCULATION"
    EVIDENCE_OFFER = "EVIDENCE_OFFER"
    EVIDENCE_ATTACK = "EVIDENCE_ATTACK"
    PROCEDURAL = "PROCEDURAL"
    BACKGROUND_NARRATIVE = "BACKGROUND_NARRATIVE"
    REQUEST_RELIEF = "REQUEST_RELIEF"
    OTHER = "OTHER"


class ParagraphAnalysisModel(BaseModel):
    keywords: list[str] = Field(min_length=5, max_length=15)
    issues: list[str] = Field(min_length=1, max_length=5)
    role: RoleEnum
    summary_3_sentences: str
    continuation_of_previous: bool
    continuation_reason: str | None = None
    citations_norms: list[str] = []
    citations_cases: list[str] = []
    citations_contract: list[str] = []
    citations_exhibits: list[str] = []

    @field_validator("summary_3_sentences")
    @classmethod
    def validate_three_sentences(cls, value: str):
        parts = [x for x in value.replace("!", ".").replace("?", ".").split(".") if x.strip()]
        if len(parts) != 3:
            raise ValueError("summary_3_sentences must contain exactly 3 sentences")
        return value


class LinkTypeEnum(str, Enum):
    ATTACKS_FACTS = "ATTACKS_FACTS"
    ATTACKS_CALCULATION = "ATTACKS_CALCULATION"
    COUNTERS_LEGAL_VIEW = "COUNTERS_LEGAL_VIEW"
    RAISES_DEFENSE = "RAISES_DEFENSE"
    DISTINGUISHES = "DISTINGUISHES"
    SUPPORTS = "SUPPORTS"
    CONCEDES = "CONCEDES"
    OFFERS_EVIDENCE = "OFFERS_EVIDENCE"
    PROCEDURAL = "PROCEDURAL"
    NOT_A_RESPONSE = "NOT_A_RESPONSE"


class LinkProposalModel(BaseModel):
    link_type: LinkTypeEnum
    confidence: float = Field(ge=0.0, le=1.0)
    rationale_short: str = Field(max_length=300)
