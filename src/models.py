from typing import Literal
from pydantic import BaseModel, Field


class Judgment(BaseModel):
    is_leaky: bool = Field(..., description="Whether the post contains data leakage")
    severity_score: float = Field(
        ..., ge=0.0, le=1.0, description="Severity score from 0.0 to 1.0"
    )
    confidence: float = Field(
        ..., ge=0.0, le=1.0, description="Confidence in the judgment from 0.0 to 1.0"
    )


class Classification(BaseModel):
    leakage_domain: Literal["Semantic", "Orthographic", "Syntactic", "Structural"] = (
        Field(..., description="The domain of the leakage")
    )
    specific_mechanism: str = Field(
        ..., description="The specific mechanism of the leakage"
    )
    is_novel_category: bool = Field(
        ..., description="Whether the mechanism is a novel category"
    )
    definition: str = Field(..., description="Short description of the mechanism")


class Forensics(BaseModel):
    evidence_spans: list[str] = Field(
        ..., description="List of exact strings that constitute evidence"
    )
    evidence_location: Literal["Beginning", "Middle", "End", "Scattered", "N/A"] = (
        Field(..., description="Location of the evidence in the text")
    )
    pattern_abstraction: str = Field(
        ..., description="Technical description of the pattern logic"
    )


class JudgeResult(BaseModel):
    judgment: Judgment = Field(..., description="The overall judgment")
    classification: Classification | None = Field(
        None,
        description="Classification of the leakage (only present if is_leaky=true)",
    )
    forensics: Forensics | None = Field(
        None,
        description="Forensic analysis of the leakage (only present if is_leaky=true)",
    )
