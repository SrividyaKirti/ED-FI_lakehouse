"""Generate realistic Ed-Fi XML files simulating the Grand Bend ISD district.

Usage (from repo root)::

    python -m data_generation.generate_edfi_xml [--output-dir data/bronze/edfi]
                                                 [--num-students 5000]

This produces 10 XML interchange files that the PySpark Bronze-layer parser
will ingest.  Certain records are intentionally planted with data-quality
issues (null IDs, invalid references, future dates, grade-level violations)
so the DQ-gate pipeline has something to catch.
"""

from __future__ import annotations

import math
import os
import random
from datetime import date, timedelta
from typing import Dict, List, Optional, Tuple

from faker import Faker
from lxml import etree

from data_generation.reference_data import (
    get_learning_standards,
    get_misconception_patterns,
    get_school_registry,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
NS = "http://ed-fi.org/0220"
NSMAP = {None: NS}

SCHOOL_YEAR = "2025-2026"
ENTRY_DATE = "2025-08-15"
SCHOOL_START = date(2025, 8, 15)
SCHOOL_END = date(2026, 5, 22)

GRADE_DESCRIPTORS = {
    0: "Kindergarten",
    1: "First grade",
    2: "Second grade",
    3: "Third grade",
    4: "Fourth grade",
    5: "Fifth grade",
    6: "Sixth grade",
    7: "Seventh grade",
    8: "Eighth grade",
    9: "Ninth grade",
    10: "Tenth grade",
    11: "Eleventh grade",
    12: "Twelfth grade",
}

# Math course names per grade level
MATH_COURSES = {
    0: "Kindergarten Math",
    1: "Math 1",
    2: "Math 2",
    3: "Math 3",
    4: "Math 4",
    5: "Math 5",
    6: "Math 6",
    7: "Math 7",
    8: "Pre-Algebra",
    9: "Algebra I",
    10: "Geometry",
    11: "Algebra II",
    12: "Pre-Calculus",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _se(parent: etree._Element, tag: str, text: Optional[str] = None) -> etree._Element:
    """Create a sub-element with optional text content."""
    elem = etree.SubElement(parent, f"{{{NS}}}{tag}")
    if text is not None:
        elem.text = str(text)
    return elem


def _make_root(interchange_name: str) -> etree._Element:
    """Create the root <InterchangeXxx> element with the Ed-Fi namespace."""
    return etree.Element(f"{{{NS}}}{interchange_name}", nsmap=NSMAP)


def _write_tree(root: etree._Element, filepath: str) -> None:
    """Write an lxml tree to a file with pretty-printing."""
    tree = etree.ElementTree(root)
    tree.write(
        filepath,
        xml_declaration=True,
        encoding="UTF-8",
        pretty_print=True,
    )


def _school_days(start: date, end: date) -> List[date]:
    """Return weekdays between start and end (inclusive)."""
    days = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Mon-Fri
            days.append(current)
        current += timedelta(days=1)
    return days


def _birth_date_for_grade(grade: int, fake: Faker) -> str:
    """Generate a plausible birth date for a student in the given grade level.

    A kindergartener (grade 0) is typically 5 turning 6 during the 2025-2026
    school year.
    """
    age = 5 + grade
    # Born between Aug of birth_year-1 and Jul of birth_year
    birth_year = 2025 - age
    start_date = date(birth_year - 1, 8, 1)
    end_date = date(birth_year, 7, 31)
    return str(fake.date_between(start_date=start_date, end_date=end_date))


# ---------------------------------------------------------------------------
# Grand Bend ISD school helpers
# ---------------------------------------------------------------------------
def _get_gb_schools() -> List[dict]:
    """Return only Grand Bend ISD schools from the registry."""
    return [
        s for s in get_school_registry()
        if s["source_system"] == "edfi"
    ]


def _schools_for_grade(grade: int, schools: List[dict]) -> List[dict]:
    """Return schools whose grade band includes the given grade level."""
    return [
        s for s in schools
        if s["grade_band_low"] <= grade <= s["grade_band_high"]
    ]


# ---------------------------------------------------------------------------
# XML Generator Functions
# ---------------------------------------------------------------------------


def _generate_students(
    output_dir: str, num_students: int, fake: Faker, rng: random.Random
) -> List[dict]:
    """Generate Students.xml and return student metadata list."""
    root = _make_root("InterchangeStudentIdentity")
    students = []

    # Determine which students get null IDs (~2%)
    null_id_indices = set(rng.sample(
        range(num_students),
        max(1, int(num_students * 0.02)),
    ))

    for i in range(num_students):
        grade = rng.randint(0, 12)
        student_id = f"STU-{i + 1:05d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        birth = _birth_date_for_grade(grade, fake)
        email = f"{first_name.lower()}.{last_name.lower()}@grandbend.edu"

        student_el = _se(root, "Student")

        uid_el = _se(student_el, "StudentUniqueId")
        if i in null_id_indices:
            uid_el.text = ""  # planted null ID
        else:
            uid_el.text = student_id

        _se(student_el, "FirstName", first_name)
        _se(student_el, "LastSurname", last_name)
        _se(student_el, "BirthDate", birth)
        _se(student_el, "Email", email)

        students.append({
            "index": i,
            "student_id": student_id if i not in null_id_indices else "",
            "first_name": first_name,
            "last_name": last_name,
            "birth_date": birth,
            "email": email,
            "grade_level": grade,
            "has_null_id": i in null_id_indices,
        })

    _write_tree(root, os.path.join(output_dir, "Students.xml"))
    return students


def _generate_schools(output_dir: str) -> List[dict]:
    """Generate Schools.xml from reference data."""
    schools = _get_gb_schools()
    root = _make_root("InterchangeEducationOrganization")

    for school in schools:
        school_el = _se(root, "School")
        _se(school_el, "SchoolId", school["school_id"])
        _se(school_el, "SchoolName", school["school_name"])
        _se(school_el, "SchoolType", school["school_type"])
        _se(school_el, "GradeBandLow", str(school["grade_band_low"]))
        _se(school_el, "GradeBandHigh", str(school["grade_band_high"]))
        _se(school_el, "DistrictId", school["district_id"])
        _se(school_el, "DistrictName", school["district_name"])

    _write_tree(root, os.path.join(output_dir, "Schools.xml"))
    return schools


def _generate_staff(
    output_dir: str, num_staff: int, fake: Faker
) -> List[dict]:
    """Generate Staff.xml with teacher records."""
    root = _make_root("InterchangeStaffAssociation")
    staff_list = []

    for i in range(num_staff):
        staff_id = f"TCH-{i + 1:05d}"
        first_name = fake.first_name()
        last_name = fake.last_name()
        email = f"{first_name.lower()}.{last_name.lower()}@grandbend.edu"

        staff_el = _se(root, "Staff")
        _se(staff_el, "StaffUniqueId", staff_id)
        _se(staff_el, "FirstName", first_name)
        _se(staff_el, "LastSurname", last_name)
        _se(staff_el, "Email", email)

        staff_list.append({
            "staff_id": staff_id,
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
        })

    _write_tree(root, os.path.join(output_dir, "Staff.xml"))
    return staff_list


def _generate_sections(
    output_dir: str,
    schools: List[dict],
    rng: random.Random,
    target_sections: int = 400,
) -> List[dict]:
    """Generate Sections.xml — sections distributed across schools and grade bands."""
    root = _make_root("InterchangeMasterSchedule")
    sections = []

    # Distribute sections roughly evenly across schools, proportional to grade span
    total_grade_slots = sum(
        s["grade_band_high"] - s["grade_band_low"] + 1 for s in schools
    )
    section_counter = 0

    for school in schools:
        grade_low = school["grade_band_low"]
        grade_high = school["grade_band_high"]
        num_grades = grade_high - grade_low + 1
        # Proportional allocation
        school_sections = max(
            num_grades,
            int(target_sections * num_grades / total_grade_slots),
        )

        for grade in range(grade_low, grade_high + 1):
            # Multiple sections per grade
            sections_per_grade = max(1, school_sections // num_grades)
            for s in range(sections_per_grade):
                section_counter += 1
                section_id = f"SEC-{section_counter:05d}"
                course_name = MATH_COURSES.get(grade, f"Math {grade}")
                curriculum_version = rng.choice(["A", "B"])

                section_el = _se(root, "Section")
                _se(section_el, "SectionIdentifier", section_id)
                course_ref = _se(section_el, "CourseOfferingReference")
                _se(course_ref, "CourseName", course_name)
                _se(course_ref, "GradeLevel", str(grade))
                school_ref = _se(section_el, "SchoolReference")
                _se(school_ref, "SchoolId", school["school_id"])
                _se(section_el, "CurriculumVersion", curriculum_version)

                sections.append({
                    "section_id": section_id,
                    "school_id": school["school_id"],
                    "grade_level": grade,
                    "course_name": course_name,
                    "curriculum_version": curriculum_version,
                })

    _write_tree(root, os.path.join(output_dir, "Sections.xml"))
    return sections


def _generate_student_school_associations(
    output_dir: str,
    students: List[dict],
    schools: List[dict],
    rng: random.Random,
) -> List[dict]:
    """Generate StudentSchoolAssociations.xml with planted DQ issues.

    Planted issues:
    - 15-20 records with invalid SchoolId (scaled for small datasets)
    - 5-10 grade-level violations (K-2 students at high school)
    - 5 records with future EntryDate (2027-09-01)
    """
    root = _make_root("InterchangeStudentEnrollment")
    associations = []
    num = len(students)

    # Scale planted issues for small datasets
    num_invalid_school = max(2, min(20, int(num * 0.04)))
    num_grade_violations = max(1, min(10, int(num * 0.02)))
    num_future_dates = max(1, min(5, int(num * 0.02)))

    # Pick indices for planted issues (non-overlapping, skip null-id students)
    valid_indices = [
        i for i, s in enumerate(students) if not s["has_null_id"]
    ]
    rng.shuffle(valid_indices)

    invalid_school_indices = set(valid_indices[:num_invalid_school])
    grade_violation_indices = set(
        valid_indices[num_invalid_school:num_invalid_school + num_grade_violations]
    )
    future_date_indices = set(
        valid_indices[
            num_invalid_school + num_grade_violations:
            num_invalid_school + num_grade_violations + num_future_dates
        ]
    )

    for i, student in enumerate(students):
        grade = student["grade_level"]
        student_id = student["student_id"]

        # Determine school assignment
        if i in invalid_school_indices:
            school_id = "INVALID-SCH-999"
        elif i in grade_violation_indices and grade <= 2:
            # Assign K-2 student to high school (grade-level violation)
            hs = [s for s in schools if s["school_type"] == "High"]
            school_id = hs[0]["school_id"] if hs else schools[-1]["school_id"]
        elif i in grade_violation_indices:
            # If the student isn't K-2, still create a violation: assign to wrong type
            elem_schools = [s for s in schools if s["school_type"] == "Elementary"]
            if grade >= 9 and elem_schools:
                school_id = elem_schools[0]["school_id"]
            else:
                # Fall back to normal assignment
                eligible = _schools_for_grade(grade, schools)
                school_id = rng.choice(eligible)["school_id"] if eligible else schools[0]["school_id"]
        else:
            eligible = _schools_for_grade(grade, schools)
            if eligible:
                school_id = rng.choice(eligible)["school_id"]
            else:
                school_id = schools[0]["school_id"]

        # Entry date
        if i in future_date_indices:
            entry_date = "2027-09-01"
        else:
            entry_date = ENTRY_DATE

        assoc_el = _se(root, "StudentSchoolAssociation")
        stu_ref = _se(assoc_el, "StudentReference")
        _se(stu_ref, "StudentUniqueId", student_id)
        sch_ref = _se(assoc_el, "SchoolReference")
        _se(sch_ref, "SchoolId", school_id)
        _se(assoc_el, "EntryDate", entry_date)
        _se(
            assoc_el,
            "EntryGradeLevelDescriptor",
            GRADE_DESCRIPTORS.get(grade, f"Grade {grade}"),
        )

        associations.append({
            "student_index": i,
            "student_id": student_id,
            "school_id": school_id,
            "grade_level": grade,
            "entry_date": entry_date,
            "is_invalid_school": i in invalid_school_indices,
            "is_grade_violation": i in grade_violation_indices,
            "is_future_date": i in future_date_indices,
        })

    _write_tree(root, os.path.join(output_dir, "StudentSchoolAssociations.xml"))
    return associations


def _generate_student_section_associations(
    output_dir: str,
    students: List[dict],
    associations: List[dict],
    sections: List[dict],
    rng: random.Random,
) -> List[dict]:
    """Generate StudentSectionAssociations.xml with planted dangling references.

    Each student is assigned to one math section in their school for their grade.
    5 records get a dangling SectionIdentifier (scaled for small datasets).
    """
    root = _make_root("InterchangeStudentEnrollment")
    student_section_assocs = []

    # Build lookup: (school_id, grade_level) -> list of section_ids
    section_lookup: Dict[Tuple[str, int], List[str]] = {}
    for sec in sections:
        key = (sec["school_id"], sec["grade_level"])
        section_lookup.setdefault(key, []).append(sec["section_id"])

    num = len(students)
    num_dangling = max(1, min(5, int(num * 0.02)))

    # Pick indices for dangling references
    valid_indices = [
        i for i, a in enumerate(associations)
        if not a["is_invalid_school"]
    ]
    dangling_indices = set(rng.sample(
        valid_indices,
        min(num_dangling, len(valid_indices)),
    ))

    for i, (student, assoc) in enumerate(zip(students, associations)):
        # Skip students with invalid school references (they have no real school)
        if assoc["is_invalid_school"]:
            continue

        school_id = assoc["school_id"]
        grade = student["grade_level"]
        student_id = student["student_id"]

        if i in dangling_indices:
            section_id = "DANGLING-SEC-999"
        else:
            key = (school_id, grade)
            available = section_lookup.get(key, [])
            if available:
                section_id = rng.choice(available)
            else:
                # If no exact match, try any section in the school
                school_secs = [
                    s["section_id"] for s in sections
                    if s["school_id"] == school_id
                ]
                section_id = rng.choice(school_secs) if school_secs else sections[0]["section_id"]

        assoc_el = _se(root, "StudentSectionAssociation")
        stu_ref = _se(assoc_el, "StudentReference")
        _se(stu_ref, "StudentUniqueId", student_id)
        sec_ref = _se(assoc_el, "SectionReference")
        _se(sec_ref, "SectionIdentifier", section_id)
        sch_ref = _se(sec_ref, "SchoolReference")
        _se(sch_ref, "SchoolId", school_id)

        student_section_assocs.append({
            "student_id": student_id,
            "section_id": section_id,
            "school_id": school_id,
            "grade_level": grade,
            "is_dangling": i in dangling_indices,
        })

    _write_tree(root, os.path.join(output_dir, "StudentSectionAssociations.xml"))
    return student_section_assocs


def _generate_grades(
    output_dir: str,
    student_section_assocs: List[dict],
    rng: random.Random,
) -> None:
    """Generate Grades.xml — numeric grading-period grades per student per section."""
    root = _make_root("InterchangeStudentGrade")
    grading_periods = ["Fall Semester", "Spring Semester"]

    for ssa in student_section_assocs:
        if ssa["is_dangling"]:
            continue

        for period in grading_periods:
            grade_el = _se(root, "Grade")
            stu_ref = _se(grade_el, "StudentReference")
            _se(stu_ref, "StudentUniqueId", ssa["student_id"])
            sec_ref = _se(grade_el, "SectionReference")
            _se(sec_ref, "SectionIdentifier", ssa["section_id"])
            _se(grade_el, "GradingPeriod", period)
            # Score: normal distribution centered on 75, std dev 12
            score = max(0, min(100, int(rng.gauss(75, 12))))
            _se(grade_el, "NumericGrade", str(score))

    _write_tree(root, os.path.join(output_dir, "Grades.xml"))


def _generate_student_assessments(
    output_dir: str,
    students: List[dict],
    student_section_assocs: List[dict],
    rng: random.Random,
) -> None:
    """Generate StudentAssessments.xml with item-level responses and misconceptions.

    Each assessment has 5 questions aligned to a learning standard.
    For specific standards, clusters of 8-12 students share the same wrong
    answer pattern (misconception).
    """
    root = _make_root("InterchangeStudentAssessment")
    standards = get_learning_standards()
    misconception_patterns = get_misconception_patterns()

    # Build lookup: grade_level -> list of standards
    standards_by_grade: Dict[int, List[dict]] = {}
    for std in standards:
        standards_by_grade.setdefault(std["grade_level"], []).append(std)

    # Build misconception lookup: standard_code -> misconception info
    misconception_by_standard: Dict[str, dict] = {
        m["standard_code"]: m for m in misconception_patterns
    }

    # Identify misconception target standards
    misconception_standard_codes = {
        "CCSS.MATH.4.OA.A.1",
        "CCSS.MATH.3.NF.A.1",
        "CCSS.MATH.4.NF.B.3",
    }

    # Build student-to-section mapping
    student_to_section: Dict[str, dict] = {}
    for ssa in student_section_assocs:
        if not ssa["is_dangling"]:
            student_to_section[ssa["student_id"]] = ssa

    # Pre-assign misconception clusters: for each target standard,
    # pick 8-12 students in the appropriate grade to share the same wrong answer.
    # Scale for small datasets: minimum 3 students per cluster.
    eligible_by_standard: Dict[str, List[str]] = {}
    for std_code in misconception_standard_codes:
        std_info = next(
            (s for s in standards if s["standard_code"] == std_code), None
        )
        if std_info is None:
            continue
        target_grade = std_info["grade_level"]
        eligible = [
            s["student_id"]
            for s in students
            if s["grade_level"] == target_grade
            and s["student_id"] != ""
            and s["student_id"] in student_to_section
        ]
        cluster_size = max(3, min(12, len(eligible)))
        if eligible:
            cluster = rng.sample(eligible, min(cluster_size, len(eligible)))
            eligible_by_standard[std_code] = cluster

    # Generate wrong answer values for each misconception cluster
    misconception_wrong_answers: Dict[str, List[str]] = {}
    for std_code in misconception_standard_codes:
        mc = misconception_by_standard.get(std_code)
        if mc is None:
            continue
        # Generate 5 fixed "wrong answers" that the cluster shares
        if mc["pattern_label"] == "subtraction_instead_of_division":
            misconception_wrong_answers[std_code] = ["3", "5", "2", "7", "4"]
        elif mc["pattern_label"] == "numerator_denominator_swap":
            misconception_wrong_answers[std_code] = ["4/3", "5/2", "3/1", "8/5", "7/4"]
        elif mc["pattern_label"] == "fraction_addition_whole_number":
            misconception_wrong_answers[std_code] = ["2/7", "3/9", "2/5", "4/11", "3/7"]
        else:
            misconception_wrong_answers[std_code] = ["wrong1", "wrong2", "wrong3", "wrong4", "wrong5"]

    # Correct answer templates per standard (for generating items)
    def _correct_answers_for_standard(std_code: str) -> List[str]:
        """Return 5 plausible correct answers for assessment items."""
        if "OA" in std_code:
            return ["12", "8", "15", "24", "6"]
        elif "NF" in std_code:
            return ["3/4", "2/5", "1/3", "5/8", "4/7"]
        elif "NBT" in std_code:
            return ["45", "78", "120", "93", "56"]
        elif "CC" in std_code:
            return ["10", "20", "5", "15", "8"]
        return ["A", "B", "C", "D", "A"]

    assessment_counter = 0
    for student in students:
        if student["student_id"] == "" or student["student_id"] not in student_to_section:
            continue

        grade = student["grade_level"]
        grade_standards = standards_by_grade.get(grade, [])
        if not grade_standards:
            # Use nearest lower grade's standards
            for g in range(grade - 1, -1, -1):
                grade_standards = standards_by_grade.get(g, [])
                if grade_standards:
                    break
        if not grade_standards:
            continue

        # Pick 1-2 standards for this student's assessment
        num_assessments = min(len(grade_standards), rng.randint(1, 2))
        assessed_standards = rng.sample(grade_standards, num_assessments)

        for std in assessed_standards:
            assessment_counter += 1
            std_code = std["standard_code"]

            assessment_el = _se(root, "StudentAssessment")
            _se(assessment_el, "AssessmentIdentifier", f"ASMT-{assessment_counter:06d}")
            stu_ref = _se(assessment_el, "StudentReference")
            _se(stu_ref, "StudentUniqueId", student["student_id"])
            _se(assessment_el, "AssessmentTitle", f"Math Assessment - {std['domain']}")
            _se(assessment_el, "StandardCode", std_code)
            _se(assessment_el, "GradeLevel", str(grade))

            # Overall score
            score = max(0, min(100, int(rng.gauss(72, 15))))
            _se(assessment_el, "ScoreResult", str(score))

            correct_answers = _correct_answers_for_standard(std_code)

            # Check if this student is in a misconception cluster for this standard
            in_cluster = (
                std_code in eligible_by_standard
                and student["student_id"] in eligible_by_standard[std_code]
            )

            mc_info = misconception_by_standard.get(std_code)

            # Generate 5 items
            for q_idx in range(5):
                item_el = _se(assessment_el, "StudentAssessmentItem")
                _se(item_el, "QuestionNumber", str(q_idx + 1))
                _se(item_el, "LearningStandardCode", std_code)
                _se(item_el, "CorrectAnswer", correct_answers[q_idx])

                if in_cluster and mc_info:
                    # This student is in the misconception cluster:
                    # give the shared wrong answer for this question
                    wrong = misconception_wrong_answers[std_code][q_idx]
                    _se(item_el, "StudentAnswer", wrong)
                    _se(item_el, "MisconceptionIndicator", mc_info["pattern_label"])
                else:
                    # Random answer: 70% chance correct, 30% wrong
                    if rng.random() < 0.70:
                        _se(item_el, "StudentAnswer", correct_answers[q_idx])
                        # No misconception indicator for correct answers
                    else:
                        # Random wrong answer (not matching misconception pattern)
                        wrong_val = str(rng.randint(1, 50))
                        _se(item_el, "StudentAnswer", wrong_val)
                        # No misconception indicator for random wrong answers

    _write_tree(root, os.path.join(output_dir, "StudentAssessments.xml"))


def _generate_attendance(
    output_dir: str,
    students: List[dict],
    associations: List[dict],
    rng: random.Random,
) -> None:
    """Generate StudentSchoolAttendanceEvents.xml.

    180 school days, ~93% present rate overall.
    Some students have much lower attendance (~75-85%) for the early warning system.
    """
    root = _make_root("InterchangeStudentAttendance")

    school_days = _school_days(SCHOOL_START, SCHOOL_END)
    # Limit to 180 days
    if len(school_days) > 180:
        school_days = school_days[:180]

    statuses = ["Present", "Absent", "Tardy", "Excused Absence"]
    # Weights for normal attendance (~93% present)
    normal_weights = [0.93, 0.03, 0.02, 0.02]
    # Weights for low-attendance students (~80% present)
    low_weights = [0.80, 0.10, 0.05, 0.05]

    num_students = len(students)
    # ~5% of students have low attendance
    num_low_attendance = max(1, int(num_students * 0.05))
    low_attendance_indices = set(rng.sample(range(num_students), num_low_attendance))

    for i, student in enumerate(students):
        if student["student_id"] == "":
            continue

        # Check if this student has a valid school association
        assoc = associations[i]
        if assoc["is_invalid_school"]:
            continue

        weights = low_weights if i in low_attendance_indices else normal_weights

        for day in school_days:
            status = rng.choices(statuses, weights=weights, k=1)[0]

            event_el = _se(root, "StudentSchoolAttendanceEvent")
            stu_ref = _se(event_el, "StudentReference")
            _se(stu_ref, "StudentUniqueId", student["student_id"])
            sch_ref = _se(event_el, "SchoolReference")
            _se(sch_ref, "SchoolId", assoc["school_id"])
            _se(event_el, "EventDate", str(day))
            _se(event_el, "AttendanceEventCategory", status)

    _write_tree(root, os.path.join(output_dir, "StudentSchoolAttendanceEvents.xml"))


def _generate_learning_standards(output_dir: str) -> None:
    """Generate LearningStandards.xml from reference data."""
    root = _make_root("InterchangeLearningStandards")
    standards = get_learning_standards()

    for std in standards:
        std_el = _se(root, "LearningStandard")
        _se(std_el, "StandardCode", std["standard_code"])
        _se(std_el, "StandardDescription", std["standard_description"])
        _se(std_el, "Domain", std["domain"])
        _se(std_el, "GradeLevel", str(std["grade_level"]))
        if std["prerequisite_standard_code"]:
            _se(std_el, "PrerequisiteStandardCode", std["prerequisite_standard_code"])

    _write_tree(root, os.path.join(output_dir, "LearningStandards.xml"))


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def generate_edfi_district(output_dir: str, num_students: int = 5000) -> None:
    """Generate all 10 Ed-Fi XML interchange files for Grand Bend ISD.

    Parameters
    ----------
    output_dir : str
        Directory where the XML files will be written. Created if needed.
    num_students : int
        Number of student records to generate (default 5000).
    """
    os.makedirs(output_dir, exist_ok=True)

    # Seed for reproducibility
    fake = Faker()
    Faker.seed(42)
    rng = random.Random(42)

    # Scale staff/sections proportionally
    num_staff = max(10, int(num_students * 0.04))
    target_sections = max(20, int(num_students * 0.08))

    # 1. Schools (from reference data)
    schools = _generate_schools(output_dir)

    # 2. Students
    students = _generate_students(output_dir, num_students, fake, rng)

    # 3. Staff
    _generate_staff(output_dir, num_staff, fake)

    # 4. Sections
    sections = _generate_sections(output_dir, schools, rng, target_sections)

    # 5. StudentSchoolAssociations (with planted DQ issues)
    associations = _generate_student_school_associations(
        output_dir, students, schools, rng
    )

    # 6. StudentSectionAssociations (with dangling references)
    student_section_assocs = _generate_student_section_associations(
        output_dir, students, associations, sections, rng
    )

    # 7. Grades
    _generate_grades(output_dir, student_section_assocs, rng)

    # 8. StudentAssessments (with misconceptions)
    _generate_student_assessments(
        output_dir, students, student_section_assocs, rng
    )

    # 9. Attendance
    _generate_attendance(output_dir, students, associations, rng)

    # 10. LearningStandards
    _generate_learning_standards(output_dir)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate Ed-Fi XML data")
    parser.add_argument(
        "--output-dir",
        default="data/bronze/edfi",
        help="Output directory for XML files (default: data/bronze/edfi)",
    )
    parser.add_argument(
        "--num-students",
        type=int,
        default=5000,
        help="Number of students to generate (default: 5000)",
    )
    args = parser.parse_args()
    generate_edfi_district(args.output_dir, args.num_students)
    print(f"Generated Ed-Fi XML files in {args.output_dir}")
