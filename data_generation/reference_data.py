"""Static reference data: school registry, learning standards, and misconception patterns.

This module is the single source of truth for all reference/seed data used
throughout the project.  Every list-of-dicts returned here can be written to a
CSV with ``write_seeds.py`` and loaded into dbt as a seed.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# School Registry
# ---------------------------------------------------------------------------

def get_school_registry() -> list[dict]:
    """Return the full school registry for both districts.

    Grand Bend ISD  (Ed-Fi source)  — 8 schools
    Riverside USD    (OneRoster source) — 6 schools
    """
    schools: list[dict] = []

    # -- Grand Bend ISD (Ed-Fi) -------------------------------------------------
    gb_district = {
        "district_id": "GB-ISD",
        "district_name": "Grand Bend ISD",
        "source_system": "edfi",
    }
    for i in range(1, 6):
        schools.append(
            {
                "school_id": f"GB-ES-{i:03d}",
                "school_name": f"Grand Bend Elementary {i}",
                "school_type": "Elementary",
                "grade_band_low": 0,
                "grade_band_high": 5,
                **gb_district,
            }
        )
    for i in range(1, 3):
        schools.append(
            {
                "school_id": f"GB-MS-{i:03d}",
                "school_name": f"Grand Bend Middle {i}",
                "school_type": "Middle",
                "grade_band_low": 6,
                "grade_band_high": 8,
                **gb_district,
            }
        )
    schools.append(
        {
            "school_id": "GB-HS-001",
            "school_name": "Grand Bend High",
            "school_type": "High",
            "grade_band_low": 9,
            "grade_band_high": 12,
            **gb_district,
        }
    )

    # -- Riverside USD (OneRoster) -----------------------------------------------
    rv_district = {
        "district_id": "RV-USD",
        "district_name": "Riverside USD",
        "source_system": "oneroster",
    }
    for i in range(1, 5):
        schools.append(
            {
                "school_id": f"RV-ES-{i:03d}",
                "school_name": f"Riverside Elementary {i}",
                "school_type": "Elementary",
                "grade_band_low": 0,
                "grade_band_high": 5,
                **rv_district,
            }
        )
    schools.append(
        {
            "school_id": "RV-MS-001",
            "school_name": "Riverside Middle",
            "school_type": "Middle",
            "grade_band_low": 6,
            "grade_band_high": 8,
            **rv_district,
        }
    )
    schools.append(
        {
            "school_id": "RV-HS-001",
            "school_name": "Riverside High",
            "school_type": "High",
            "grade_band_low": 9,
            "grade_band_high": 12,
            **rv_district,
        }
    )

    return schools


# ---------------------------------------------------------------------------
# Learning Standards — CCSS Math K-5
# ---------------------------------------------------------------------------

def get_learning_standards() -> list[dict]:
    """Return ~23 CCSS Math standards spanning grades K-5 with prerequisite chains.

    Domains covered
    ---------------
    CC  — Counting & Cardinality (K only)
    OA  — Operations & Algebraic Thinking (K-5)
    NBT — Number & Operations in Base Ten (1-5)
    NF  — Number & Operations—Fractions (3-5)

    Prerequisite chains
    -------------------
    A long chain threads through multiple grade levels so downstream
    generators can model mastery progression::

        K.CC.A.1 -> K.CC.B.4 -> K.OA.A.1 -> 1.OA.A.1 -> 1.OA.B.3
        -> 2.OA.A.1 -> 2.NBT.B.5 -> 3.OA.A.1 -> 3.NF.A.1
        -> 4.OA.A.1 -> 4.NF.B.3 -> 5.NF.A.1 -> 5.NF.B.3
    """
    # fmt: off
    standards = [
        # ── Kindergarten (grade_level 0) ── domain: Counting & Cardinality ──
        {
            "standard_code": "CCSS.MATH.K.CC.A.1",
            "standard_description": "Count to 100 by ones and by tens",
            "domain": "Counting & Cardinality",
            "grade_level": 0,
            "prerequisite_standard_code": "",
        },
        {
            "standard_code": "CCSS.MATH.K.CC.A.2",
            "standard_description": "Count forward beginning from a given number within the known sequence",
            "domain": "Counting & Cardinality",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.A.1",
        },
        {
            "standard_code": "CCSS.MATH.K.CC.B.4",
            "standard_description": "Understand the relationship between numbers and quantities; connect counting to cardinality",
            "domain": "Counting & Cardinality",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.A.1",
        },
        # ── Kindergarten — domain: Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.K.OA.A.1",
            "standard_description": "Represent addition and subtraction with objects, fingers, mental images, drawings, sounds, acting out situations, verbal explanations, expressions, or equations",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.B.4",
        },
        {
            "standard_code": "CCSS.MATH.K.OA.A.2",
            "standard_description": "Solve addition and subtraction word problems, and add and subtract within 10",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.OA.A.1",
        },
        # ── Grade 1 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.1.OA.A.1",
            "standard_description": "Use addition and subtraction within 20 to solve word problems",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.K.OA.A.1",
        },
        {
            "standard_code": "CCSS.MATH.1.OA.B.3",
            "standard_description": "Apply properties of operations as strategies to add and subtract",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.1.OA.A.1",
        },
        # ── Grade 1 — Number & Operations in Base Ten ──
        {
            "standard_code": "CCSS.MATH.1.NBT.B.2",
            "standard_description": "Understand that the two digits of a two-digit number represent amounts of tens and ones",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.B.4",
        },
        {
            "standard_code": "CCSS.MATH.1.NBT.C.4",
            "standard_description": "Add within 100, including adding a two-digit number and a one-digit number",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.1.NBT.B.2",
        },
        # ── Grade 2 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.2.OA.A.1",
            "standard_description": "Use addition and subtraction within 100 to solve one- and two-step word problems",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.MATH.1.OA.B.3",
        },
        {
            "standard_code": "CCSS.MATH.2.OA.B.2",
            "standard_description": "Fluently add and subtract within 20 using mental strategies",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.MATH.2.OA.A.1",
        },
        # ── Grade 2 — Number & Operations in Base Ten ──
        {
            "standard_code": "CCSS.MATH.2.NBT.B.5",
            "standard_description": "Fluently add and subtract within 100 using strategies based on place value",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.MATH.2.OA.A.1",
        },
        # ── Grade 3 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.3.OA.A.1",
            "standard_description": "Interpret products of whole numbers, e.g., interpret 5 x 7 as the total number of objects in 5 groups of 7 objects each",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.2.NBT.B.5",
        },
        {
            "standard_code": "CCSS.MATH.3.OA.A.3",
            "standard_description": "Use multiplication and division within 100 to solve word problems",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.3.OA.A.1",
        },
        # ── Grade 3 — Number & Operations in Base Ten ──
        {
            "standard_code": "CCSS.MATH.3.NBT.A.2",
            "standard_description": "Fluently add and subtract within 1000 using strategies and algorithms based on place value",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.2.NBT.B.5",
        },
        # ── Grade 3 — Fractions ──
        {
            "standard_code": "CCSS.MATH.3.NF.A.1",
            "standard_description": "Understand a fraction 1/b as the quantity formed by 1 part when a whole is partitioned into b equal parts",
            "domain": "Number & Operations-Fractions",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.3.OA.A.1",
        },
        # ── Grade 4 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.4.OA.A.1",
            "standard_description": "Interpret a multiplication equation as a comparison; represent verbal statements of multiplicative comparisons as multiplication equations",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.3.NF.A.1",
        },
        {
            "standard_code": "CCSS.MATH.4.OA.A.2",
            "standard_description": "Multiply or divide to solve word problems involving multiplicative comparison",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.4.OA.A.1",
        },
        # ── Grade 4 — Fractions ──
        {
            "standard_code": "CCSS.MATH.4.NF.A.1",
            "standard_description": "Explain why a fraction a/b is equivalent to a fraction (n x a)/(n x b) using visual fraction models",
            "domain": "Number & Operations-Fractions",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.3.NF.A.1",
        },
        {
            "standard_code": "CCSS.MATH.4.NF.B.3",
            "standard_description": "Understand a fraction a/b with a > 1 as a sum of fractions 1/b; add and subtract fractions with like denominators",
            "domain": "Number & Operations-Fractions",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.4.OA.A.1",
        },
        # ── Grade 5 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.5.OA.A.1",
            "standard_description": "Use parentheses, brackets, or braces in numerical expressions, and evaluate expressions with these symbols",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.MATH.4.OA.A.2",
        },
        # ── Grade 5 — Fractions ──
        {
            "standard_code": "CCSS.MATH.5.NF.A.1",
            "standard_description": "Add and subtract fractions with unlike denominators by replacing given fractions with equivalent fractions",
            "domain": "Number & Operations-Fractions",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.MATH.4.NF.B.3",
        },
        {
            "standard_code": "CCSS.MATH.5.NF.B.3",
            "standard_description": "Interpret a fraction as division of the numerator by the denominator (a/b = a / b)",
            "domain": "Number & Operations-Fractions",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.MATH.5.NF.A.1",
        },
    ]
    # fmt: on

    return standards


# ---------------------------------------------------------------------------
# Misconception Patterns
# ---------------------------------------------------------------------------

def get_misconception_patterns() -> list[dict]:
    """Return common math misconception patterns tied to specific standards."""
    return [
        {
            "misconception_id": "MC-001",
            "standard_code": "CCSS.MATH.4.OA.A.1",
            "pattern_label": "subtraction_instead_of_division",
            "description": (
                "Student confuses 'how many fewer' (subtraction) with "
                "'how many times as many' (multiplicative comparison), "
                "applying subtraction when division or multiplication is required."
            ),
            "suggested_reteach": (
                "Use bar-model diagrams to contrast additive vs. multiplicative "
                "comparison.  Have students restate the question in their own words "
                "before choosing an operation."
            ),
            "wrong_answer_pattern": "result = a - b instead of a / b or a * b",
        },
        {
            "misconception_id": "MC-002",
            "standard_code": "CCSS.MATH.3.NF.A.1",
            "pattern_label": "numerator_denominator_swap",
            "description": (
                "Student reverses the roles of numerator and denominator, "
                "e.g., interpreting 3/4 as '4 parts out of 3' instead of "
                "'3 parts out of 4'."
            ),
            "suggested_reteach": (
                "Provide fraction strips and region models.  Label the denominator "
                "as 'how many equal parts the whole is split into' and the numerator "
                "as 'how many parts we are talking about'."
            ),
            "wrong_answer_pattern": "writes b/a when a/b is intended",
        },
        {
            "misconception_id": "MC-003",
            "standard_code": "CCSS.MATH.4.NF.B.3",
            "pattern_label": "fraction_addition_whole_number",
            "description": (
                "Student adds both numerators and denominators as if they were "
                "whole numbers, e.g., 1/3 + 1/4 = 2/7 instead of finding a "
                "common denominator."
            ),
            "suggested_reteach": (
                "Use visual area models to show that 1/3 and 1/4 refer to "
                "different-sized pieces.  Practice finding common denominators "
                "before combining."
            ),
            "wrong_answer_pattern": "a/b + c/d = (a+c)/(b+d)",
        },
        {
            "misconception_id": "MC-004",
            "standard_code": "CCSS.MATH.5.NF.B.3",
            "pattern_label": "fraction_division_invert_wrong",
            "description": (
                "When dividing fractions, the student inverts (takes the "
                "reciprocal of) the dividend instead of the divisor, e.g., "
                "computing (3/4) / (2/5) as (4/3) * (2/5) instead of "
                "(3/4) * (5/2)."
            ),
            "suggested_reteach": (
                "Emphasise the 'Keep-Change-Flip' mnemonic: keep the first "
                "fraction, change division to multiplication, flip only the "
                "second fraction.  Use number-line models to verify."
            ),
            "wrong_answer_pattern": "(a/b) / (c/d) = (b/a) * (c/d) instead of (a/b) * (d/c)",
        },
    ]
