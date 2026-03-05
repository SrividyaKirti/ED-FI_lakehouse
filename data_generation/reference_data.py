"""Static reference data: school registry, learning standards, and misconception patterns.

This module is the single source of truth for all reference/seed data used
throughout the project.  Every list-of-dicts returned here can be written to a
CSV with ``write_seeds.py`` and loaded into dbt as a seed.
"""

from __future__ import annotations


# ---------------------------------------------------------------------------
# School Registry — K-5 Elementary Only
# ---------------------------------------------------------------------------

def get_school_registry() -> list[dict]:
    """Return the K-5 school registry for both districts.

    Grand Bend ISD  (Ed-Fi source)     — 5 elementary schools
    Riverside USD   (OneRoster source)  — 4 elementary schools
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

    return schools


# ---------------------------------------------------------------------------
# Learning Standards — CCSS Math K-5, CCSS ELA K-5, NGSS K-5
# ---------------------------------------------------------------------------

def get_learning_standards() -> list[dict]:
    """Return learning standards spanning grades K-5 across Math, ELA, and Science.

    Math  — ~23 CCSS Math standards with prerequisite chains
    ELA   — ~18 CCSS ELA standards (3 per grade: Reading, Writing, Language)
    Science — ~12 NGSS standards (2 per grade)
    """
    # fmt: off
    standards = [
        # ══════════════════════════════════════════════════════════════════════
        # MATH — CCSS Math K-5
        # ══════════════════════════════════════════════════════════════════════

        # ── Kindergarten (grade_level 0) ── domain: Counting & Cardinality ──
        {
            "standard_code": "CCSS.MATH.K.CC.A.1",
            "standard_description": "Count to 100 by ones and by tens",
            "domain": "Counting & Cardinality",
            "grade_level": 0,
            "prerequisite_standard_code": "",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.K.CC.A.2",
            "standard_description": "Count forward beginning from a given number within the known sequence",
            "domain": "Counting & Cardinality",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.A.1",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.K.CC.B.4",
            "standard_description": "Understand the relationship between numbers and quantities; connect counting to cardinality",
            "domain": "Counting & Cardinality",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.A.1",
            "subject": "Math",
        },
        # ── Kindergarten — domain: Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.K.OA.A.1",
            "standard_description": "Represent addition and subtraction with objects, fingers, mental images, drawings, sounds, acting out situations, verbal explanations, expressions, or equations",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.B.4",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.K.OA.A.2",
            "standard_description": "Solve addition and subtraction word problems, and add and subtract within 10",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.MATH.K.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 1 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.1.OA.A.1",
            "standard_description": "Use addition and subtraction within 20 to solve word problems",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.K.OA.A.1",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.1.OA.B.3",
            "standard_description": "Apply properties of operations as strategies to add and subtract",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.1.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 1 — Number & Operations in Base Ten ──
        {
            "standard_code": "CCSS.MATH.1.NBT.B.2",
            "standard_description": "Understand that the two digits of a two-digit number represent amounts of tens and ones",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.K.CC.B.4",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.1.NBT.C.4",
            "standard_description": "Add within 100, including adding a two-digit number and a one-digit number",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.MATH.1.NBT.B.2",
            "subject": "Math",
        },
        # ── Grade 2 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.2.OA.A.1",
            "standard_description": "Use addition and subtraction within 100 to solve one- and two-step word problems",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.MATH.1.OA.B.3",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.2.OA.B.2",
            "standard_description": "Fluently add and subtract within 20 using mental strategies",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.MATH.2.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 2 — Number & Operations in Base Ten ──
        {
            "standard_code": "CCSS.MATH.2.NBT.B.5",
            "standard_description": "Fluently add and subtract within 100 using strategies based on place value",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.MATH.2.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 3 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.3.OA.A.1",
            "standard_description": "Interpret products of whole numbers, e.g., interpret 5 x 7 as the total number of objects in 5 groups of 7 objects each",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.2.NBT.B.5",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.3.OA.A.3",
            "standard_description": "Use multiplication and division within 100 to solve word problems",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.3.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 3 — Number & Operations in Base Ten ──
        {
            "standard_code": "CCSS.MATH.3.NBT.A.2",
            "standard_description": "Fluently add and subtract within 1000 using strategies and algorithms based on place value",
            "domain": "Number & Operations in Base Ten",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.2.NBT.B.5",
            "subject": "Math",
        },
        # ── Grade 3 — Fractions ──
        {
            "standard_code": "CCSS.MATH.3.NF.A.1",
            "standard_description": "Understand a fraction 1/b as the quantity formed by 1 part when a whole is partitioned into b equal parts",
            "domain": "Number & Operations-Fractions",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.MATH.3.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 4 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.4.OA.A.1",
            "standard_description": "Interpret a multiplication equation as a comparison; represent verbal statements of multiplicative comparisons as multiplication equations",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.3.NF.A.1",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.4.OA.A.2",
            "standard_description": "Multiply or divide to solve word problems involving multiplicative comparison",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.4.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 4 — Fractions ──
        {
            "standard_code": "CCSS.MATH.4.NF.A.1",
            "standard_description": "Explain why a fraction a/b is equivalent to a fraction (n x a)/(n x b) using visual fraction models",
            "domain": "Number & Operations-Fractions",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.3.NF.A.1",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.4.NF.B.3",
            "standard_description": "Understand a fraction a/b with a > 1 as a sum of fractions 1/b; add and subtract fractions with like denominators",
            "domain": "Number & Operations-Fractions",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.MATH.4.OA.A.1",
            "subject": "Math",
        },
        # ── Grade 5 — Operations & Algebraic Thinking ──
        {
            "standard_code": "CCSS.MATH.5.OA.A.1",
            "standard_description": "Use parentheses, brackets, or braces in numerical expressions, and evaluate expressions with these symbols",
            "domain": "Operations & Algebraic Thinking",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.MATH.4.OA.A.2",
            "subject": "Math",
        },
        # ── Grade 5 — Fractions ──
        {
            "standard_code": "CCSS.MATH.5.NF.A.1",
            "standard_description": "Add and subtract fractions with unlike denominators by replacing given fractions with equivalent fractions",
            "domain": "Number & Operations-Fractions",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.MATH.4.NF.B.3",
            "subject": "Math",
        },
        {
            "standard_code": "CCSS.MATH.5.NF.B.3",
            "standard_description": "Interpret a fraction as division of the numerator by the denominator (a/b = a / b)",
            "domain": "Number & Operations-Fractions",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.MATH.5.NF.A.1",
            "subject": "Math",
        },

        # ══════════════════════════════════════════════════════════════════════
        # ELA — CCSS ELA K-5 (3 per grade = 18 total)
        # ══════════════════════════════════════════════════════════════════════

        # ── Kindergarten ELA ──
        {
            "standard_code": "CCSS.ELA.K.RF.A.1",
            "standard_description": "Demonstrate understanding of the organization and basic features of print, including recognizing all uppercase and lowercase letters",
            "domain": "Reading: Foundational Skills",
            "grade_level": 0,
            "prerequisite_standard_code": "",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.K.RL.A.1",
            "standard_description": "With prompting and support, ask and answer questions about key details in a text",
            "domain": "Reading: Literature",
            "grade_level": 0,
            "prerequisite_standard_code": "CCSS.ELA.K.RF.A.1",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.K.W.A.2",
            "standard_description": "Use a combination of drawing, dictating, and writing to compose informative/explanatory texts",
            "domain": "Writing",
            "grade_level": 0,
            "prerequisite_standard_code": "",
            "subject": "ELA",
        },
        # ── Grade 1 ELA ──
        {
            "standard_code": "CCSS.ELA.1.RL.A.1",
            "standard_description": "Ask and answer questions about key details in a text",
            "domain": "Reading: Literature",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.ELA.K.RL.A.1",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.1.RI.A.2",
            "standard_description": "Identify the main topic and retell key details of a text",
            "domain": "Reading: Informational Text",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.ELA.K.RL.A.1",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.1.L.A.1",
            "standard_description": "Demonstrate command of the conventions of standard English grammar and usage when writing or speaking",
            "domain": "Language",
            "grade_level": 1,
            "prerequisite_standard_code": "CCSS.ELA.K.W.A.2",
            "subject": "ELA",
        },
        # ── Grade 2 ELA ──
        {
            "standard_code": "CCSS.ELA.2.RI.A.2",
            "standard_description": "Identify the main topic of a multi-paragraph text as well as the focus of specific paragraphs",
            "domain": "Reading: Informational Text",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.ELA.1.RI.A.2",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.2.RL.A.3",
            "standard_description": "Describe how characters in a story respond to major events and challenges",
            "domain": "Reading: Literature",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.ELA.1.RL.A.1",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.2.W.A.1",
            "standard_description": "Write opinion pieces in which they introduce the topic, state an opinion, supply reasons, and provide a concluding statement",
            "domain": "Writing",
            "grade_level": 2,
            "prerequisite_standard_code": "CCSS.ELA.1.L.A.1",
            "subject": "ELA",
        },
        # ── Grade 3 ELA ──
        {
            "standard_code": "CCSS.ELA.3.RL.A.3",
            "standard_description": "Describe characters in a story and explain how their actions contribute to the sequence of events",
            "domain": "Reading: Literature",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.ELA.2.RL.A.3",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.3.RI.A.2",
            "standard_description": "Determine the main idea of a text; recount the key details and explain how they support the main idea",
            "domain": "Reading: Informational Text",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.ELA.2.RI.A.2",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.3.L.A.1",
            "standard_description": "Demonstrate command of the conventions of standard English grammar including subject-verb agreement",
            "domain": "Language",
            "grade_level": 3,
            "prerequisite_standard_code": "CCSS.ELA.2.W.A.1",
            "subject": "ELA",
        },
        # ── Grade 4 ELA ──
        {
            "standard_code": "CCSS.ELA.4.RL.A.2",
            "standard_description": "Determine a theme of a story, drama, or poem from details in the text; summarize the text",
            "domain": "Reading: Literature",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.ELA.3.RL.A.3",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.4.W.A.1",
            "standard_description": "Write opinion pieces on topics or texts, supporting a point of view with reasons and information",
            "domain": "Writing",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.ELA.3.L.A.1",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.4.RI.A.3",
            "standard_description": "Explain events, procedures, ideas, or concepts in a historical, scientific, or technical text",
            "domain": "Reading: Informational Text",
            "grade_level": 4,
            "prerequisite_standard_code": "CCSS.ELA.3.RI.A.2",
            "subject": "ELA",
        },
        # ── Grade 5 ELA ──
        {
            "standard_code": "CCSS.ELA.5.RI.A.2",
            "standard_description": "Determine two or more main ideas of a text and explain how they are supported by key details; provide a summary",
            "domain": "Reading: Informational Text",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.ELA.4.RI.A.3",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.5.RL.A.2",
            "standard_description": "Determine a theme of a story, drama, or poem from details in the text, including how characters respond to challenges",
            "domain": "Reading: Literature",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.ELA.4.RL.A.2",
            "subject": "ELA",
        },
        {
            "standard_code": "CCSS.ELA.5.W.A.1",
            "standard_description": "Write opinion pieces on topics or texts, supporting a point of view with reasons and information organized logically",
            "domain": "Writing",
            "grade_level": 5,
            "prerequisite_standard_code": "CCSS.ELA.4.W.A.1",
            "subject": "ELA",
        },

        # ══════════════════════════════════════════════════════════════════════
        # SCIENCE — NGSS K-5 (2 per grade = 12 total)
        # ══════════════════════════════════════════════════════════════════════

        # ── Kindergarten Science ──
        {
            "standard_code": "NGSS.K-PS2-1",
            "standard_description": "Plan and conduct an investigation to compare the effects of different strengths or different directions of pushes and pulls on the motion of an object",
            "domain": "Physical Science",
            "grade_level": 0,
            "prerequisite_standard_code": "",
            "subject": "Science",
        },
        {
            "standard_code": "NGSS.K-ESS3-1",
            "standard_description": "Use a model to represent the relationship between the needs of different plants and animals and the places they live",
            "domain": "Earth & Space Science",
            "grade_level": 0,
            "prerequisite_standard_code": "",
            "subject": "Science",
        },
        # ── Grade 1 Science ──
        {
            "standard_code": "NGSS.1-LS1-1",
            "standard_description": "Use materials to design a solution to a human problem by mimicking how plants and/or animals use their external parts to help them survive",
            "domain": "Life Science",
            "grade_level": 1,
            "prerequisite_standard_code": "NGSS.K-ESS3-1",
            "subject": "Science",
        },
        {
            "standard_code": "NGSS.1-PS4-1",
            "standard_description": "Plan and conduct investigations to provide evidence that vibrating materials can make sound and that sound can make materials vibrate",
            "domain": "Physical Science",
            "grade_level": 1,
            "prerequisite_standard_code": "NGSS.K-PS2-1",
            "subject": "Science",
        },
        # ── Grade 2 Science ──
        {
            "standard_code": "NGSS.2-PS1-1",
            "standard_description": "Plan and conduct an investigation to describe and classify different kinds of materials by their observable properties",
            "domain": "Physical Science",
            "grade_level": 2,
            "prerequisite_standard_code": "NGSS.1-PS4-1",
            "subject": "Science",
        },
        {
            "standard_code": "NGSS.2-LS4-1",
            "standard_description": "Make observations of plants and animals to compare the diversity of life in different habitats",
            "domain": "Life Science",
            "grade_level": 2,
            "prerequisite_standard_code": "NGSS.1-LS1-1",
            "subject": "Science",
        },
        # ── Grade 3 Science ──
        {
            "standard_code": "NGSS.3-LS1-1",
            "standard_description": "Develop models to describe that organisms have unique and diverse life cycles but all have in common birth, growth, reproduction, and death",
            "domain": "Life Science",
            "grade_level": 3,
            "prerequisite_standard_code": "NGSS.2-LS4-1",
            "subject": "Science",
        },
        {
            "standard_code": "NGSS.3-PS2-1",
            "standard_description": "Plan and conduct an investigation to provide evidence of the effects of balanced and unbalanced forces on the motion of an object",
            "domain": "Physical Science",
            "grade_level": 3,
            "prerequisite_standard_code": "NGSS.2-PS1-1",
            "subject": "Science",
        },
        # ── Grade 4 Science ──
        {
            "standard_code": "NGSS.4-ESS1-1",
            "standard_description": "Identify evidence from patterns in rock formations and fossils in rock layers to support an explanation for changes in a landscape over time",
            "domain": "Earth & Space Science",
            "grade_level": 4,
            "prerequisite_standard_code": "NGSS.3-LS1-1",
            "subject": "Science",
        },
        {
            "standard_code": "NGSS.4-PS3-1",
            "standard_description": "Use evidence to construct an explanation relating the speed of an object to the energy of that object",
            "domain": "Physical Science",
            "grade_level": 4,
            "prerequisite_standard_code": "NGSS.3-PS2-1",
            "subject": "Science",
        },
        # ── Grade 5 Science ──
        {
            "standard_code": "NGSS.5-PS1-1",
            "standard_description": "Develop a model to describe that matter is made of particles too small to be seen",
            "domain": "Physical Science",
            "grade_level": 5,
            "prerequisite_standard_code": "NGSS.4-PS3-1",
            "subject": "Science",
        },
        {
            "standard_code": "NGSS.5-ESS1-1",
            "standard_description": "Support an argument that differences in the apparent brightness of the sun compared to other stars is due to their relative distances from Earth",
            "domain": "Earth & Space Science",
            "grade_level": 5,
            "prerequisite_standard_code": "NGSS.4-ESS1-1",
            "subject": "Science",
        },
    ]
    # fmt: on

    return standards


# ---------------------------------------------------------------------------
# Misconception Patterns — Math, ELA, Science
# ---------------------------------------------------------------------------

def get_misconception_patterns() -> list[dict]:
    """Return common misconception patterns tied to specific standards."""
    return [
        # ── Math misconceptions ──
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
        # ── ELA misconceptions ──
        {
            "misconception_id": "MC-ELA-001",
            "standard_code": "CCSS.ELA.3.RI.A.2",
            "pattern_label": "main_idea_vs_detail",
            "description": (
                "Student confuses supporting details with the main idea, "
                "selecting a specific detail or example when asked to identify "
                "the central message of the passage."
            ),
            "suggested_reteach": (
                "Use graphic organizers (main idea webs) where students place "
                "the big idea in the center and supporting details around it. "
                "Practice with short paragraphs before full passages."
            ),
            "wrong_answer_pattern": "selects supporting detail instead of main idea",
        },
        {
            "misconception_id": "MC-ELA-002",
            "standard_code": "CCSS.ELA.4.RL.A.2",
            "pattern_label": "literal_vs_inferential",
            "description": (
                "Student answers with text-explicit (literal) information when "
                "the question requires making an inference. Struggles to read "
                "between the lines or draw conclusions beyond stated facts."
            ),
            "suggested_reteach": (
                "Model think-aloud strategies: 'The text says... I know that... "
                "So I can figure out...' Use sentence stems for inference practice "
                "with progressively less scaffolding."
            ),
            "wrong_answer_pattern": "provides literal text quote instead of inference",
        },
        {
            "misconception_id": "MC-ELA-003",
            "standard_code": "CCSS.ELA.4.W.A.1",
            "pattern_label": "opinion_vs_fact",
            "description": (
                "Student presents personal opinions as facts in informational "
                "or opinion writing without supporting evidence or reasoning, "
                "or confuses factual claims with opinion statements."
            ),
            "suggested_reteach": (
                "Create a two-column chart (Fact vs. Opinion) with examples. "
                "Teach signal words for opinions (I think, I believe, in my view) "
                "and practice identifying evidence-based claims."
            ),
            "wrong_answer_pattern": "states opinion as fact without evidence",
        },
        # ── Science misconceptions ──
        {
            "misconception_id": "MC-SCI-001",
            "standard_code": "NGSS.3-LS1-1",
            "pattern_label": "plants_eat_soil",
            "description": (
                "Student believes plants get their food (mass) from the soil "
                "rather than from photosynthesis. Confuses water/mineral uptake "
                "with the primary source of plant growth."
            ),
            "suggested_reteach": (
                "Conduct a plant growth experiment comparing soil weight before "
                "and after growth. Introduce photosynthesis as 'plants making "
                "food from sunlight, water, and air (CO2)' with diagrams."
            ),
            "wrong_answer_pattern": "says plants get food from soil/roots",
        },
        {
            "misconception_id": "MC-SCI-002",
            "standard_code": "NGSS.3-PS2-1",
            "pattern_label": "heavier_falls_faster",
            "description": (
                "Student thinks heavier objects always fall faster than lighter "
                "ones, not accounting for air resistance or understanding that "
                "gravity accelerates all objects equally in a vacuum."
            ),
            "suggested_reteach": (
                "Drop objects of different masses but similar shapes (e.g., "
                "two balls of different weights) and observe. Discuss Galileo's "
                "experiment and distinguish mass from air resistance effects."
            ),
            "wrong_answer_pattern": "claims heavier objects fall faster",
        },
        {
            "misconception_id": "MC-SCI-003",
            "standard_code": "NGSS.5-ESS1-1",
            "pattern_label": "seasons_distance",
            "description": (
                "Student thinks seasons are caused by Earth's changing distance "
                "from the Sun rather than by the tilt of Earth's axis and the "
                "resulting angle of sunlight."
            ),
            "suggested_reteach": (
                "Use a globe and flashlight to model how axial tilt changes "
                "the angle and duration of sunlight. Point out that when it is "
                "summer in the Northern Hemisphere, it is winter in the Southern."
            ),
            "wrong_answer_pattern": "says seasons caused by distance from Sun",
        },
    ]
