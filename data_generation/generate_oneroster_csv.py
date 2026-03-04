"""Generate OneRoster 1.2 compliant CSV files simulating the Riverside USD district.

Usage (from repo root)::

    python -m data_generation.generate_oneroster_csv [--output-dir data/bronze/oneroster]
                                                      [--num-students 3500]

This produces 9 CSV files conforming to OneRoster 1.2 specification.
Certain records are intentionally planted with data-quality issues
(null sourcedIds, dangling references, future dates, grade-level violations)
so the DQ-gate pipeline has something to catch.
"""

from __future__ import annotations

import csv
import math
import os
import random
import uuid
from datetime import date, timedelta
from typing import Dict, List, Optional, Set, Tuple

from faker import Faker

from data_generation.reference_data import (
    get_learning_standards,
    get_misconception_patterns,
    get_school_registry,
)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SCHOOL_YEAR = "2025-2026"
SCHOOL_YEAR_LABEL = "2025-2026"
SCHOOL_START = date(2025, 8, 15)
SCHOOL_END = date(2026, 5, 22)
DATE_LAST_MODIFIED = "2025-08-01T00:00:00.000Z"

# OneRoster uses numeric grade format: "00" for K, "01" for 1st, etc.
GRADE_NUMERIC = {g: f"{g:02d}" for g in range(13)}

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
    12: "AP Calculus",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def _deterministic_uuid(seed_string: str) -> str:
    """Generate a deterministic UUID5 from a seed string."""
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, seed_string))


def _write_csv(filepath: str, headers: List[str], rows: List[List[str]]) -> None:
    """Write a CSV file conforming to RFC 4180, UTF-8."""
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_MINIMAL)
        writer.writerow(headers)
        writer.writerows(rows)


def _birth_date_for_grade(grade: int, fake: Faker) -> str:
    """Generate a plausible birth date for a student in the given grade level."""
    age = 5 + grade
    birth_year = 2025 - age
    start_date = date(birth_year - 1, 8, 1)
    end_date = date(birth_year, 7, 31)
    return str(fake.date_between(start_date=start_date, end_date=end_date))


def _get_rv_schools() -> List[dict]:
    """Return only Riverside USD schools from the registry."""
    return [
        s for s in get_school_registry()
        if s["source_system"] == "oneroster"
    ]


def _schools_for_grade(grade: int, schools: List[dict]) -> List[dict]:
    """Return schools whose grade band includes the given grade level."""
    return [
        s for s in schools
        if s["grade_band_low"] <= grade <= s["grade_band_high"]
    ]


# ---------------------------------------------------------------------------
# CSV Generator Functions
# ---------------------------------------------------------------------------

def _generate_orgs(output_dir: str, schools: List[dict]) -> Tuple[str, List[dict]]:
    """Generate orgs.csv -- districts and schools.

    Returns the district sourcedId and the list of org records.
    """
    headers = [
        "sourcedId", "status", "dateLastModified",
        "name", "type", "identifier", "parentSourcedId",
    ]

    district_sourced_id = _deterministic_uuid("RV-USD-district")

    rows = []
    # District row
    rows.append([
        district_sourced_id,
        "active",
        DATE_LAST_MODIFIED,
        "Riverside USD",
        "district",
        "RV-USD",
        "",  # no parent for district
    ])

    org_records = [{
        "sourcedId": district_sourced_id,
        "name": "Riverside USD",
        "type": "district",
        "school_id": "RV-USD",
    }]

    for school in schools:
        school_sourced_id = _deterministic_uuid(f"school-{school['school_id']}")
        rows.append([
            school_sourced_id,
            "active",
            DATE_LAST_MODIFIED,
            school["school_name"],
            "school",
            school["school_id"],
            district_sourced_id,
        ])
        org_records.append({
            "sourcedId": school_sourced_id,
            "name": school["school_name"],
            "type": "school",
            "school_id": school["school_id"],
            "grade_band_low": school["grade_band_low"],
            "grade_band_high": school["grade_band_high"],
            "school_type": school["school_type"],
        })

    _write_csv(os.path.join(output_dir, "orgs.csv"), headers, rows)
    return district_sourced_id, org_records


def _generate_academic_sessions(
    output_dir: str,
) -> Tuple[str, List[dict]]:
    """Generate academicSessions.csv -- 1 schoolYear, 2 semesters, 4 quarters."""
    headers = [
        "sourcedId", "status", "dateLastModified",
        "title", "type", "startDate", "endDate",
        "parentSourcedId", "schoolYear",
    ]

    sy_id = _deterministic_uuid("schoolYear-2025-2026")
    sem1_id = _deterministic_uuid("semester-1-2025-2026")
    sem2_id = _deterministic_uuid("semester-2-2025-2026")
    q1_id = _deterministic_uuid("quarter-1-2025-2026")
    q2_id = _deterministic_uuid("quarter-2-2025-2026")
    q3_id = _deterministic_uuid("quarter-3-2025-2026")
    q4_id = _deterministic_uuid("quarter-4-2025-2026")

    sessions = [
        {
            "sourcedId": sy_id,
            "title": "School Year 2025-2026",
            "type": "schoolYear",
            "startDate": "2025-08-15",
            "endDate": "2026-05-22",
            "parentSourcedId": "",
        },
        {
            "sourcedId": sem1_id,
            "title": "Fall Semester 2025",
            "type": "semester",
            "startDate": "2025-08-15",
            "endDate": "2025-12-19",
            "parentSourcedId": sy_id,
        },
        {
            "sourcedId": sem2_id,
            "title": "Spring Semester 2026",
            "type": "semester",
            "startDate": "2026-01-06",
            "endDate": "2026-05-22",
            "parentSourcedId": sy_id,
        },
        {
            "sourcedId": q1_id,
            "title": "Quarter 1",
            "type": "gradingPeriod",
            "startDate": "2025-08-15",
            "endDate": "2025-10-17",
            "parentSourcedId": sem1_id,
        },
        {
            "sourcedId": q2_id,
            "title": "Quarter 2",
            "type": "gradingPeriod",
            "startDate": "2025-10-20",
            "endDate": "2025-12-19",
            "parentSourcedId": sem1_id,
        },
        {
            "sourcedId": q3_id,
            "title": "Quarter 3",
            "type": "gradingPeriod",
            "startDate": "2026-01-06",
            "endDate": "2026-03-13",
            "parentSourcedId": sem2_id,
        },
        {
            "sourcedId": q4_id,
            "title": "Quarter 4",
            "type": "gradingPeriod",
            "startDate": "2026-03-16",
            "endDate": "2026-05-22",
            "parentSourcedId": sem2_id,
        },
    ]

    rows = []
    for s in sessions:
        rows.append([
            s["sourcedId"],
            "active",
            DATE_LAST_MODIFIED,
            s["title"],
            s["type"],
            s["startDate"],
            s["endDate"],
            s["parentSourcedId"],
            SCHOOL_YEAR_LABEL,
        ])

    _write_csv(os.path.join(output_dir, "academicSessions.csv"), headers, rows)
    return sy_id, sessions


def _generate_courses(
    output_dir: str,
    schools: List[dict],
    org_records: List[dict],
    school_year_id: str,
) -> List[dict]:
    """Generate courses.csv -- course catalog for each school."""
    headers = [
        "sourcedId", "status", "dateLastModified",
        "schoolYearSourcedId", "title", "courseCode",
        "grades", "orgSourcedId", "subjects", "subjectCodes",
    ]

    # Build school_id -> orgSourcedId lookup
    school_org_lookup = {
        o["school_id"]: o["sourcedId"]
        for o in org_records
        if o["type"] == "school"
    }

    courses = []
    rows = []

    for school in schools:
        org_sourced_id = school_org_lookup[school["school_id"]]
        grade_low = school["grade_band_low"]
        grade_high = school["grade_band_high"]

        for grade in range(grade_low, grade_high + 1):
            course_name = MATH_COURSES.get(grade, "Math {}".format(grade))
            course_code = "MATH-{}".format(grade)
            course_id = _deterministic_uuid(
                "course-{}-{}".format(school["school_id"], grade)
            )

            courses.append({
                "sourcedId": course_id,
                "title": course_name,
                "courseCode": course_code,
                "grade": grade,
                "grade_numeric": GRADE_NUMERIC[grade],
                "orgSourcedId": org_sourced_id,
                "school_id": school["school_id"],
            })

            rows.append([
                course_id,
                "active",
                DATE_LAST_MODIFIED,
                school_year_id,
                course_name,
                course_code,
                GRADE_NUMERIC[grade],
                org_sourced_id,
                "Mathematics",
                "MATH",
            ])

    _write_csv(os.path.join(output_dir, "courses.csv"), headers, rows)
    return courses


def _generate_classes(
    output_dir: str,
    schools: List[dict],
    courses: List[dict],
    org_records: List[dict],
    sessions: List[dict],
    rng: random.Random,
    target_sections: int = 200,
) -> List[dict]:
    """Generate classes.csv -- sections distributed across schools and grade bands.

    Also creates AP classes for grade-level violation planting.
    """
    headers = [
        "sourcedId", "status", "dateLastModified",
        "title", "grades", "courseSourcedId", "classCode",
        "classType", "location", "schoolSourcedId",
        "termSourcedIds", "subjects", "subjectCodes", "periods",
    ]

    # Build school_id -> orgSourcedId lookup
    school_org_lookup = {
        o["school_id"]: o["sourcedId"]
        for o in org_records
        if o["type"] == "school"
    }

    # Build (school_id, grade) -> courseSourcedId lookup
    course_lookup = {}  # type: Dict[Tuple[str, int], str]
    for c in courses:
        course_lookup[(c["school_id"], c["grade"])] = c["sourcedId"]

    # Use school year term IDs
    term_ids = ",".join(s["sourcedId"] for s in sessions if s["type"] == "semester")

    classes = []
    rows = []

    # Distribute sections across schools proportional to grade span
    total_grade_slots = sum(
        s["grade_band_high"] - s["grade_band_low"] + 1 for s in schools
    )
    section_counter = 0

    for school in schools:
        grade_low = school["grade_band_low"]
        grade_high = school["grade_band_high"]
        num_grades = grade_high - grade_low + 1
        org_sourced_id = school_org_lookup[school["school_id"]]

        school_sections = max(
            num_grades,
            int(target_sections * num_grades / total_grade_slots),
        )

        for grade in range(grade_low, grade_high + 1):
            sections_per_grade = max(1, school_sections // num_grades)
            course_sourced_id = course_lookup.get(
                (school["school_id"], grade), ""
            )
            course_name = MATH_COURSES.get(grade, "Math {}".format(grade))

            for s in range(sections_per_grade):
                section_counter += 1
                curriculum_version = rng.choice(["A", "B"])
                class_code = "MATH-{}-{}".format(grade, curriculum_version)
                class_id = _deterministic_uuid(
                    "class-{}-{}-{}".format(
                        school["school_id"], grade, section_counter
                    )
                )

                class_record = {
                    "sourcedId": class_id,
                    "title": "{} Section {}".format(course_name, s + 1),
                    "grade": grade,
                    "grade_numeric": GRADE_NUMERIC[grade],
                    "courseSourcedId": course_sourced_id,
                    "classCode": class_code,
                    "schoolSourcedId": org_sourced_id,
                    "school_id": school["school_id"],
                    "curriculum_version": curriculum_version,
                }
                classes.append(class_record)

                rows.append([
                    class_id,
                    "active",
                    DATE_LAST_MODIFIED,
                    class_record["title"],
                    GRADE_NUMERIC[grade],
                    course_sourced_id,
                    class_code,
                    "scheduled",
                    "Room {}".format(section_counter),
                    org_sourced_id,
                    term_ids,
                    "Mathematics",
                    "MATH",
                    "Period {}".format((section_counter % 8) + 1),
                ])

    # Add AP classes for grade-level violation planting.
    # These are classes with "AP" in the title that K-2 students will be
    # enrolled in (the violation).  We place them at high schools.
    hs_schools = [s for s in schools if s["school_type"] == "High"]
    for hs in hs_schools:
        org_sourced_id = school_org_lookup[hs["school_id"]]
        for ap_idx, ap_name in enumerate(["AP Calculus", "AP Statistics"]):
            section_counter += 1
            ap_class_id = _deterministic_uuid(
                "ap-class-{}-{}".format(hs["school_id"], ap_idx)
            )
            # Use grade 12 course for AP
            course_sourced_id = course_lookup.get(
                (hs["school_id"], 12), ""
            )

            class_record = {
                "sourcedId": ap_class_id,
                "title": ap_name,
                "grade": 12,
                "grade_numeric": "12",
                "courseSourcedId": course_sourced_id,
                "classCode": "AP-MATH-{}".format(ap_idx + 1),
                "schoolSourcedId": org_sourced_id,
                "school_id": hs["school_id"],
                "curriculum_version": "A",
                "is_ap": True,
            }
            classes.append(class_record)

            rows.append([
                ap_class_id,
                "active",
                DATE_LAST_MODIFIED,
                ap_name,
                "12",
                course_sourced_id,
                "AP-MATH-{}".format(ap_idx + 1),
                "scheduled",
                "Room {}".format(section_counter),
                org_sourced_id,
                term_ids,
                "Mathematics",
                "MATH",
                "Period {}".format((section_counter % 8) + 1),
            ])

    _write_csv(os.path.join(output_dir, "classes.csv"), headers, rows)
    return classes


def _generate_users(
    output_dir: str,
    num_students: int,
    schools: List[dict],
    fake: Faker,
    rng: random.Random,
) -> Tuple[List[dict], List[dict]]:
    """Generate users.csv -- students + teachers.

    Returns (students, teachers) metadata lists.
    Plants ~2% of students with null sourcedId.
    """
    headers = [
        "sourcedId", "status", "dateLastModified",
        "enabledUser", "username", "givenName", "familyName",
        "middleName", "identifier", "email", "sms", "phone",
        "role", "grades", "password",
    ]

    rows = []
    students = []
    teachers = []

    # Determine which students get null sourcedIds (~2%)
    null_id_count = max(1, int(num_students * 0.02))
    null_id_indices = set(rng.sample(range(num_students), null_id_count))

    # Generate students
    for i in range(num_students):
        grade = rng.randint(0, 12)
        first_name = fake.first_name()
        last_name = fake.last_name()
        middle_name = fake.first_name()
        username = "{}.{}{}".format(
            first_name.lower(), last_name.lower(), i
        )
        email = "{}@riverside.edu".format(username)
        identifier = "RV-STU-{:05d}".format(i + 1)

        if i in null_id_indices:
            sourced_id = ""
        else:
            sourced_id = _deterministic_uuid(
                "student-{}".format(identifier)
            )

        students.append({
            "index": i,
            "sourcedId": sourced_id,
            "givenName": first_name,
            "familyName": last_name,
            "middleName": middle_name,
            "username": username,
            "email": email,
            "identifier": identifier,
            "grade_level": grade,
            "grade_numeric": GRADE_NUMERIC[grade],
            "has_null_id": i in null_id_indices,
        })

        rows.append([
            sourced_id,
            "active",
            DATE_LAST_MODIFIED,
            "true",
            username,
            first_name,
            last_name,
            middle_name,
            identifier,
            email,
            "",  # sms
            "",  # phone
            "student",
            GRADE_NUMERIC[grade],
            "",  # password
        ])

    # Generate teachers (~1 per 25 students, minimum 10)
    num_teachers = max(10, int(num_students / 25))
    for i in range(num_teachers):
        first_name = fake.first_name()
        last_name = fake.last_name()
        middle_name = fake.first_name()
        username = "{}.{}".format(first_name.lower(), last_name.lower())
        email = "{}@riverside.edu".format(username)
        identifier = "RV-TCH-{:05d}".format(i + 1)
        sourced_id = _deterministic_uuid("teacher-{}".format(identifier))

        teachers.append({
            "sourcedId": sourced_id,
            "givenName": first_name,
            "familyName": last_name,
            "username": username,
            "email": email,
            "identifier": identifier,
        })

        rows.append([
            sourced_id,
            "active",
            DATE_LAST_MODIFIED,
            "true",
            username,
            first_name,
            last_name,
            middle_name,
            identifier,
            email,
            "",  # sms
            "",  # phone
            "teacher",
            "",  # grades (not applicable for teachers)
            "",  # password
        ])

    _write_csv(os.path.join(output_dir, "users.csv"), headers, rows)
    return students, teachers


def _generate_demographics(
    output_dir: str,
    students: List[dict],
    fake: Faker,
    rng: random.Random,
) -> None:
    """Generate demographics.csv -- student demographics."""
    headers = [
        "sourcedId", "status", "dateLastModified",
        "birthDate", "sex",
        "americanIndianOrAlaskaNative", "asian",
        "blackOrAfricanAmerican",
        "nativeHawaiianOrOtherPacificIslander",
        "white", "demographicRaceTwoOrMoreRaces",
        "hispanicOrLatinoEthnicity",
    ]

    rows = []
    for student in students:
        if not student["sourcedId"]:
            continue

        birth = _birth_date_for_grade(student["grade_level"], fake)
        sex = rng.choice(["male", "female"])

        # Generate race/ethnicity flags
        race_roll = rng.random()
        if race_roll < 0.45:
            flags = ("false", "false", "false", "false", "true", "false", "false")
        elif race_roll < 0.70:
            flags = ("false", "false", "false", "false", "false", "false", "true")
        elif race_roll < 0.82:
            flags = ("false", "false", "true", "false", "false", "false", "false")
        elif race_roll < 0.90:
            flags = ("false", "true", "false", "false", "false", "false", "false")
        elif race_roll < 0.95:
            flags = ("false", "false", "false", "false", "false", "true", "false")
        else:
            flags = ("true", "false", "false", "false", "false", "false", "false")

        rows.append([
            student["sourcedId"],
            "active",
            DATE_LAST_MODIFIED,
            birth,
            sex,
            *flags,
        ])

    _write_csv(os.path.join(output_dir, "demographics.csv"), headers, rows)


def _generate_enrollments(
    output_dir: str,
    students: List[dict],
    teachers: List[dict],
    classes: List[dict],
    org_records: List[dict],
    schools: List[dict],
    rng: random.Random,
) -> List[dict]:
    """Generate enrollments.csv -- student-to-class + teacher-to-class links.

    Planted DQ issues:
    - 5+ dangling classSourcedId (referencing non-existent classes)
    - 3+ future beginDate
    - 5-10 grade-level violations (K-2 students in AP classes)
    """
    headers = [
        "sourcedId", "status", "dateLastModified",
        "classSourcedId", "schoolSourcedId", "userSourcedId",
        "role", "primary", "beginDate", "endDate",
    ]

    # Build lookups
    school_org_lookup = {
        o["school_id"]: o["sourcedId"]
        for o in org_records
        if o["type"] == "school"
    }

    # (school_id, grade) -> list of class records (non-AP)
    class_lookup = {}  # type: Dict[Tuple[str, int], List[dict]]
    for cls in classes:
        if cls.get("is_ap"):
            continue
        key = (cls["school_id"], cls["grade"])
        class_lookup.setdefault(key, []).append(cls)

    # AP classes
    ap_classes = [c for c in classes if c.get("is_ap") or "AP" in c["title"]]

    enrollments = []
    rows = []
    enrollment_counter = 0

    # --- Determine planted issue indices ---
    valid_student_indices = [
        i for i, s in enumerate(students)
        if not s["has_null_id"]
    ]

    # Dangling classSourcedId: 5+ (scaled)
    num_dangling = max(5, int(len(students) * 0.03))
    # Future beginDate: 3+ (scaled)
    num_future = max(3, int(len(students) * 0.02))
    # Grade-level violations: 5-10 K-2 students in AP classes
    k2_indices = [
        i for i in valid_student_indices
        if students[i]["grade_level"] <= 2
    ]
    num_grade_violations = min(max(5, int(len(k2_indices) * 0.15)), 10, len(k2_indices))

    rng.shuffle(valid_student_indices)
    dangling_indices = set(valid_student_indices[:num_dangling])
    future_indices = set(
        valid_student_indices[num_dangling:num_dangling + num_future]
    )

    # Pick K-2 students for grade violations (separate from dangling/future)
    used_indices = dangling_indices | future_indices
    available_k2 = [i for i in k2_indices if i not in used_indices]
    rng.shuffle(available_k2)
    grade_violation_indices = set(available_k2[:num_grade_violations])

    # --- Generate student enrollments ---
    for i, student in enumerate(students):
        if student["has_null_id"]:
            continue

        enrollment_counter += 1
        grade = student["grade_level"]
        student_sourced_id = student["sourcedId"]

        # Determine school
        eligible_schools = _schools_for_grade(grade, schools)
        if not eligible_schools:
            eligible_schools = schools
        school = rng.choice(eligible_schools)
        school_sourced_id = school_org_lookup.get(school["school_id"], "")

        # Determine class assignment
        if i in dangling_indices:
            # Dangling reference: use a fake class ID
            class_sourced_id = _deterministic_uuid(
                "dangling-class-{}".format(enrollment_counter)
            )
        elif i in grade_violation_indices and ap_classes:
            # Grade-level violation: K-2 student in AP class
            ap_class = rng.choice(ap_classes)
            class_sourced_id = ap_class["sourcedId"]
            # Use the AP class's school
            school_sourced_id = ap_class["schoolSourcedId"]
        else:
            # Normal assignment
            key = (school["school_id"], grade)
            available = class_lookup.get(key, [])
            if available:
                class_sourced_id = rng.choice(available)["sourcedId"]
            else:
                # Fallback: any class in the school
                school_classes = [
                    c for c in classes
                    if c["school_id"] == school["school_id"]
                    and not c.get("is_ap")
                ]
                if school_classes:
                    class_sourced_id = rng.choice(school_classes)["sourcedId"]
                else:
                    class_sourced_id = classes[0]["sourcedId"]

        # Determine beginDate
        if i in future_indices:
            begin_date = "2027-09-01"
        else:
            begin_date = "2025-08-15"

        end_date = "2026-05-22"

        enrollment_id = _deterministic_uuid(
            "enrollment-student-{}".format(enrollment_counter)
        )

        enrollments.append({
            "sourcedId": enrollment_id,
            "classSourcedId": class_sourced_id,
            "schoolSourcedId": school_sourced_id,
            "userSourcedId": student_sourced_id,
            "role": "student",
            "beginDate": begin_date,
            "is_dangling": i in dangling_indices,
            "is_future": i in future_indices,
            "is_grade_violation": i in grade_violation_indices,
            "grade_level": grade,
        })

        rows.append([
            enrollment_id,
            "active",
            DATE_LAST_MODIFIED,
            class_sourced_id,
            school_sourced_id,
            student_sourced_id,
            "student",
            "true",
            begin_date,
            end_date,
        ])

    # --- Generate teacher enrollments ---
    # Assign each teacher to 2-4 classes
    non_ap_classes = [c for c in classes if not c.get("is_ap")]
    for teacher in teachers:
        num_classes = rng.randint(2, min(4, len(non_ap_classes)))
        teacher_classes = rng.sample(non_ap_classes, num_classes)

        for cls in teacher_classes:
            enrollment_counter += 1
            enrollment_id = _deterministic_uuid(
                "enrollment-teacher-{}".format(enrollment_counter)
            )

            enrollments.append({
                "sourcedId": enrollment_id,
                "classSourcedId": cls["sourcedId"],
                "schoolSourcedId": cls["schoolSourcedId"],
                "userSourcedId": teacher["sourcedId"],
                "role": "teacher",
                "beginDate": "2025-08-15",
                "is_dangling": False,
                "is_future": False,
                "is_grade_violation": False,
            })

            rows.append([
                enrollment_id,
                "active",
                DATE_LAST_MODIFIED,
                cls["sourcedId"],
                cls["schoolSourcedId"],
                teacher["sourcedId"],
                "teacher",
                "true",
                "2025-08-15",
                "2026-05-22",
            ])

    _write_csv(os.path.join(output_dir, "enrollments.csv"), headers, rows)
    return enrollments


def _generate_line_items(
    output_dir: str,
    classes: List[dict],
    rng: random.Random,
) -> List[dict]:
    """Generate lineItems.csv -- ~5-10 assessments per class."""
    headers = [
        "sourcedId", "status", "dateLastModified",
        "title", "description", "assignDate", "dueDate",
        "classSourcedId", "categorySourcedId",
        "resultValueMin", "resultValueMax",
    ]

    assessment_titles = [
        "Quiz", "Homework", "Unit Test", "Midterm Exam",
        "Practice Problems", "Project", "Exit Ticket",
        "Benchmark Assessment", "Chapter Review", "Final Exam",
    ]

    category_id = _deterministic_uuid("category-math-assessment")

    line_items = []
    rows = []
    item_counter = 0

    for cls in classes:
        num_items = rng.randint(5, 10)
        for j in range(num_items):
            item_counter += 1
            li_id = _deterministic_uuid(
                "lineitem-{}-{}".format(cls["sourcedId"], j)
            )
            title = "{} {}".format(rng.choice(assessment_titles), j + 1)

            # Assign date within the school year
            days_offset = rng.randint(0, 250)
            assign_date = SCHOOL_START + timedelta(days=days_offset)
            due_date = assign_date + timedelta(days=rng.randint(1, 14))

            # Clamp to school year
            if due_date > SCHOOL_END:
                due_date = SCHOOL_END

            line_items.append({
                "sourcedId": li_id,
                "title": title,
                "classSourcedId": cls["sourcedId"],
                "class_grade": cls.get("grade", 0),
                "class_school_id": cls.get("school_id", ""),
            })

            rows.append([
                li_id,
                "active",
                DATE_LAST_MODIFIED,
                title,
                "Math assessment for {}".format(cls["title"]),
                str(assign_date),
                str(due_date),
                cls["sourcedId"],
                category_id,
                "0",
                "100",
            ])

    _write_csv(os.path.join(output_dir, "lineItems.csv"), headers, rows)
    return line_items


def _generate_results(
    output_dir: str,
    students: List[dict],
    enrollments: List[dict],
    line_items: List[dict],
    rng: random.Random,
) -> None:
    """Generate results.csv -- student scores with item-level detail.

    Extra columns: question_number, standard_code, correct_answer,
                   student_answer, misconception_indicator.

    Plants misconception patterns for the 3 target standards.
    """
    headers = [
        "sourcedId", "status", "dateLastModified",
        "lineItemSourcedId", "studentSourcedId",
        "scoreStatus", "score", "scoreDate", "comment",
        # Item-level extension columns
        "question_number", "standard_code",
        "correct_answer", "student_answer",
        "misconception_indicator",
    ]

    standards = get_learning_standards()
    misconception_patterns = get_misconception_patterns()

    # Build lookup: grade_level -> list of standards
    standards_by_grade = {}  # type: Dict[int, List[dict]]
    for std in standards:
        standards_by_grade.setdefault(std["grade_level"], []).append(std)

    # Build misconception lookup: standard_code -> misconception info
    misconception_by_standard = {
        m["standard_code"]: m for m in misconception_patterns
    }

    # Target standards for misconception planting
    misconception_standard_codes = {
        "CCSS.MATH.4.OA.A.1",
        "CCSS.MATH.3.NF.A.1",
        "CCSS.MATH.4.NF.B.3",
    }

    # Build student enrollment lookup: studentSourcedId -> enrollment info
    student_enrollments = {}  # type: Dict[str, dict]
    for e in enrollments:
        if e["role"] == "student" and not e["is_dangling"]:
            student_enrollments[e["userSourcedId"]] = e

    # Build class -> lineItems lookup
    class_line_items = {}  # type: Dict[str, List[dict]]
    for li in line_items:
        class_line_items.setdefault(li["classSourcedId"], []).append(li)

    # Pre-assign misconception clusters: for each target standard,
    # pick 8-12 students in the appropriate grade
    eligible_by_standard = {}  # type: Dict[str, List[str]]
    for std_code in misconception_standard_codes:
        std_info = next(
            (s for s in standards if s["standard_code"] == std_code), None
        )
        if std_info is None:
            continue
        target_grade = std_info["grade_level"]
        eligible = [
            s["sourcedId"]
            for s in students
            if s["grade_level"] == target_grade
            and s["sourcedId"]
            and s["sourcedId"] in student_enrollments
        ]
        cluster_size = max(3, min(12, len(eligible)))
        if eligible:
            cluster = rng.sample(eligible, min(cluster_size, len(eligible)))
            eligible_by_standard[std_code] = cluster

    # Generate wrong answer values for each misconception cluster
    misconception_wrong_answers = {}  # type: Dict[str, List[str]]
    for std_code in misconception_standard_codes:
        mc = misconception_by_standard.get(std_code)
        if mc is None:
            continue
        if mc["pattern_label"] == "subtraction_instead_of_division":
            misconception_wrong_answers[std_code] = ["3", "5", "2", "7", "4"]
        elif mc["pattern_label"] == "numerator_denominator_swap":
            misconception_wrong_answers[std_code] = ["4/3", "5/2", "3/1", "8/5", "7/4"]
        elif mc["pattern_label"] == "fraction_addition_whole_number":
            misconception_wrong_answers[std_code] = ["2/7", "3/9", "2/5", "4/11", "3/7"]
        else:
            misconception_wrong_answers[std_code] = [
                "wrong1", "wrong2", "wrong3", "wrong4", "wrong5"
            ]

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

    rows = []
    result_counter = 0

    for student in students:
        if not student["sourcedId"]:
            continue
        if student["sourcedId"] not in student_enrollments:
            continue

        enrollment = student_enrollments[student["sourcedId"]]
        class_sourced_id = enrollment["classSourcedId"]
        grade = student["grade_level"]

        # Get line items for this class
        items_for_class = class_line_items.get(class_sourced_id, [])
        if not items_for_class:
            continue

        # Get standards for this grade
        grade_standards = standards_by_grade.get(grade, [])
        if not grade_standards:
            for g in range(grade - 1, -1, -1):
                grade_standards = standards_by_grade.get(g, [])
                if grade_standards:
                    break

        if not grade_standards:
            continue

        # Pick a subset of line items to generate results for
        num_results = min(len(items_for_class), rng.randint(3, 6))
        selected_items = rng.sample(items_for_class, num_results)

        for li in selected_items:
            # Pick a standard for this result
            std = rng.choice(grade_standards)
            std_code = std["standard_code"]

            # Overall score
            score = max(0, min(100, int(rng.gauss(72, 15))))

            # Score date within school year
            days_offset = rng.randint(0, 250)
            score_date = SCHOOL_START + timedelta(days=days_offset)
            if score_date > SCHOOL_END:
                score_date = SCHOOL_END

            correct_answers = _correct_answers_for_standard(std_code)

            # Check if this student is in a misconception cluster for this standard
            in_cluster = (
                std_code in eligible_by_standard
                and student["sourcedId"] in eligible_by_standard[std_code]
            )
            mc_info = misconception_by_standard.get(std_code)

            # Generate 5 item-level result rows per assessment
            for q_idx in range(5):
                result_counter += 1
                result_id = _deterministic_uuid(
                    "result-{}-{}-{}".format(
                        student["sourcedId"], li["sourcedId"], q_idx
                    )
                )

                if in_cluster and mc_info and std_code in misconception_wrong_answers:
                    student_answer = misconception_wrong_answers[std_code][q_idx]
                    misconception_indicator = mc_info["pattern_label"]
                else:
                    if rng.random() < 0.70:
                        student_answer = correct_answers[q_idx]
                        misconception_indicator = ""
                    else:
                        student_answer = str(rng.randint(1, 50))
                        misconception_indicator = ""

                rows.append([
                    result_id,
                    "active",
                    DATE_LAST_MODIFIED,
                    li["sourcedId"],
                    student["sourcedId"],
                    "fully graded",
                    str(score),
                    str(score_date),
                    "",  # comment
                    str(q_idx + 1),
                    std_code,
                    correct_answers[q_idx],
                    student_answer,
                    misconception_indicator,
                ])

    _write_csv(os.path.join(output_dir, "results.csv"), headers, rows)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def generate_oneroster_district(output_dir: str, num_students: int = 3500) -> None:
    """Generate all 9 OneRoster CSV files for Riverside USD.

    Parameters
    ----------
    output_dir : str
        Directory where the CSV files will be written. Created if needed.
    num_students : int
        Number of student records to generate (default 3500).
    """
    os.makedirs(output_dir, exist_ok=True)

    # Seed for reproducibility (different from Ed-Fi's 42)
    fake = Faker()
    fake.seed_instance(43)
    rng = random.Random(43)

    # Get Riverside USD schools from reference data
    schools = _get_rv_schools()

    # Scale sections proportionally
    target_sections = max(20, int(num_students * 0.06))

    # 1. Orgs (districts + schools)
    district_sourced_id, org_records = _generate_orgs(output_dir, schools)

    # 2. Academic Sessions
    school_year_id, sessions = _generate_academic_sessions(output_dir)

    # 3. Courses
    courses = _generate_courses(
        output_dir, schools, org_records, school_year_id
    )

    # 4. Classes (sections)
    classes = _generate_classes(
        output_dir, schools, courses, org_records, sessions, rng,
        target_sections,
    )

    # 5. Users (students + teachers)
    students, teachers = _generate_users(
        output_dir, num_students, schools, fake, rng
    )

    # 6. Demographics
    _generate_demographics(output_dir, students, fake, rng)

    # 7. Enrollments (with planted DQ issues)
    enrollments = _generate_enrollments(
        output_dir, students, teachers, classes, org_records, schools, rng
    )

    # 8. Line Items (assignments/assessments)
    line_items = _generate_line_items(output_dir, classes, rng)

    # 9. Results (with misconception patterns)
    _generate_results(
        output_dir, students, enrollments, line_items, rng
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate OneRoster CSV data"
    )
    parser.add_argument(
        "--output-dir",
        default="data/bronze/oneroster",
        help="Output directory for CSV files (default: data/bronze/oneroster)",
    )
    parser.add_argument(
        "--num-students",
        type=int,
        default=3500,
        help="Number of students to generate (default: 3500)",
    )
    args = parser.parse_args()
    generate_oneroster_district(args.output_dir, args.num_students)
    print("Generated OneRoster CSV files in {}".format(args.output_dir))
