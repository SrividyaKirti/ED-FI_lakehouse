"""Parse Ed-Fi XML interchange files into flat PySpark DataFrames and write as Parquet.

This module implements the Bronze layer of the lakehouse pipeline: it reads
raw Ed-Fi XML files (produced by the data generator), flattens them into
tabular DataFrames, and writes them as Parquet for downstream ingestion.

Usage::

    from pyspark.sql import SparkSession
    from spark_jobs.parse_edfi_xml import run_all

    spark = SparkSession.builder.getOrCreate()
    run_all(spark, "data/bronze/edfi", "data/bronze/parquet")
"""

from __future__ import annotations

import os
import xml.etree.ElementTree as ET
from datetime import datetime
from typing import Any, Dict, List, Optional

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

# ---------------------------------------------------------------------------
# XML namespace used by the Ed-Fi 0220 interchange files
# ---------------------------------------------------------------------------
NS = "http://ed-fi.org/0220"
_NS = {"ns": NS}


def _tag(name: str) -> str:
    """Return a fully-qualified XML tag name within the Ed-Fi namespace."""
    return f"{{{NS}}}{name}"


def _text(element: Optional[ET.Element], tag: str) -> Optional[str]:
    """Extract the text content of a child element, or None if missing."""
    if element is None:
        return None
    child = element.find(_tag(tag))
    if child is not None and child.text:
        return child.text.strip() if child.text else None
    return None


def _meta_fields() -> Dict[str, str]:
    """Return the standard metadata fields added to every row."""
    return {
        "_source_system": "edfi",
        "_loaded_at": datetime.now().isoformat(),
    }


def _make_schema(columns: List[str]) -> StructType:
    """Build a StructType where every field is a nullable StringType."""
    return StructType([StructField(c, StringType(), True) for c in columns])


# ---------------------------------------------------------------------------
# Individual parse functions
# ---------------------------------------------------------------------------


def parse_students(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse Students.xml into a flat DataFrame.

    Columns: student_unique_id, first_name, last_name, birth_date, email,
             _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "Students.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for student in root.findall(_tag("Student")):
        rows.append({
            "student_unique_id": _text(student, "StudentUniqueId"),
            "first_name": _text(student, "FirstName"),
            "last_name": _text(student, "LastSurname"),
            "birth_date": _text(student, "BirthDate"),
            "email": _text(student, "Email"),
            **meta,
        })

    schema = _make_schema([
        "student_unique_id", "first_name", "last_name", "birth_date",
        "email", "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_schools(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse Schools.xml into a flat DataFrame.

    Columns: school_id, school_name, school_type, district_id, district_name,
             _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "Schools.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for school in root.findall(_tag("School")):
        rows.append({
            "school_id": _text(school, "SchoolId"),
            "school_name": _text(school, "SchoolName"),
            "school_type": _text(school, "SchoolType"),
            "district_id": _text(school, "DistrictId"),
            "district_name": _text(school, "DistrictName"),
            **meta,
        })

    schema = _make_schema([
        "school_id", "school_name", "school_type", "district_id",
        "district_name", "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_staff(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse Staff.xml into a flat DataFrame.

    Columns: staff_unique_id, first_name, last_name, email,
             _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "Staff.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for staff in root.findall(_tag("Staff")):
        rows.append({
            "staff_unique_id": _text(staff, "StaffUniqueId"),
            "first_name": _text(staff, "FirstName"),
            "last_name": _text(staff, "LastSurname"),
            "email": _text(staff, "Email"),
            **meta,
        })

    schema = _make_schema([
        "staff_unique_id", "first_name", "last_name", "email",
        "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_sections(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse Sections.xml into a flat DataFrame.

    Columns: section_identifier, school_id, course_name, curriculum_version,
             term_name, _source_system, _loaded_at

    Note: term_name is not present in the generated XML and will be None.
    """
    filepath = os.path.join(input_dir, "Sections.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for section in root.findall(_tag("Section")):
        course_ref = section.find(_tag("CourseOfferingReference"))
        school_ref = section.find(_tag("SchoolReference"))

        rows.append({
            "section_identifier": _text(section, "SectionIdentifier"),
            "school_id": _text(school_ref, "SchoolId") if school_ref is not None else None,
            "course_name": _text(course_ref, "CourseName") if course_ref is not None else None,
            "curriculum_version": _text(section, "CurriculumVersion"),
            "term_name": None,  # Not present in current XML schema
            **meta,
        })

    schema = _make_schema([
        "section_identifier", "school_id", "course_name",
        "curriculum_version", "term_name", "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_enrollments(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse StudentSchoolAssociations.xml into a flat DataFrame.

    Columns: student_unique_id, school_id, entry_date,
             grade_level_descriptor, _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "StudentSchoolAssociations.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for assoc in root.findall(_tag("StudentSchoolAssociation")):
        stu_ref = assoc.find(_tag("StudentReference"))
        sch_ref = assoc.find(_tag("SchoolReference"))

        rows.append({
            "student_unique_id": _text(stu_ref, "StudentUniqueId") if stu_ref is not None else None,
            "school_id": _text(sch_ref, "SchoolId") if sch_ref is not None else None,
            "entry_date": _text(assoc, "EntryDate"),
            "grade_level_descriptor": _text(assoc, "EntryGradeLevelDescriptor"),
            **meta,
        })

    schema = _make_schema([
        "student_unique_id", "school_id", "entry_date",
        "grade_level_descriptor", "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_section_associations(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse StudentSectionAssociations.xml into a flat DataFrame.

    Columns: student_unique_id, section_identifier, begin_date,
             _source_system, _loaded_at

    Note: begin_date is not present in the generated XML and will be None.
    """
    filepath = os.path.join(input_dir, "StudentSectionAssociations.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for assoc in root.findall(_tag("StudentSectionAssociation")):
        stu_ref = assoc.find(_tag("StudentReference"))
        sec_ref = assoc.find(_tag("SectionReference"))

        section_id = None
        if sec_ref is not None:
            section_id = _text(sec_ref, "SectionIdentifier")

        rows.append({
            "student_unique_id": _text(stu_ref, "StudentUniqueId") if stu_ref is not None else None,
            "section_identifier": section_id,
            "begin_date": None,  # Not present in current XML schema
            **meta,
        })

    schema = _make_schema([
        "student_unique_id", "section_identifier", "begin_date",
        "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_grades(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse Grades.xml into a flat DataFrame.

    Columns: student_unique_id, section_identifier, grading_period,
             numeric_grade, _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "Grades.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for grade in root.findall(_tag("Grade")):
        stu_ref = grade.find(_tag("StudentReference"))
        sec_ref = grade.find(_tag("SectionReference"))

        rows.append({
            "student_unique_id": _text(stu_ref, "StudentUniqueId") if stu_ref is not None else None,
            "section_identifier": _text(sec_ref, "SectionIdentifier") if sec_ref is not None else None,
            "grading_period": _text(grade, "GradingPeriod"),
            "numeric_grade": _text(grade, "NumericGrade"),
            **meta,
        })

    schema = _make_schema([
        "student_unique_id", "section_identifier", "grading_period",
        "numeric_grade", "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_assessment_results(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse StudentAssessments.xml into a flat DataFrame.

    Each assessment contains multiple StudentAssessmentItem elements.
    This function flattens the nested structure so that each row represents
    one item-level response, carrying the parent assessment metadata.

    Columns: student_unique_id, assessment_id, question_number,
             standard_code, correct_answer, student_answer, score,
             assessment_date, misconception_indicator,
             _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "StudentAssessments.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for assessment in root.findall(_tag("StudentAssessment")):
        stu_ref = assessment.find(_tag("StudentReference"))
        student_uid = _text(stu_ref, "StudentUniqueId") if stu_ref is not None else None
        assessment_id = _text(assessment, "AssessmentIdentifier")
        standard_code = _text(assessment, "StandardCode")
        score = _text(assessment, "ScoreResult")
        # assessment_date is not present in the generated XML
        assessment_date = None

        for item in assessment.findall(_tag("StudentAssessmentItem")):
            rows.append({
                "student_unique_id": student_uid,
                "assessment_id": assessment_id,
                "question_number": _text(item, "QuestionNumber"),
                "standard_code": _text(item, "LearningStandardCode") or standard_code,
                "correct_answer": _text(item, "CorrectAnswer"),
                "student_answer": _text(item, "StudentAnswer"),
                "score": score,
                "assessment_date": assessment_date,
                "misconception_indicator": _text(item, "MisconceptionIndicator"),
                **meta,
            })

    schema = _make_schema([
        "student_unique_id", "assessment_id", "question_number",
        "standard_code", "correct_answer", "student_answer", "score",
        "assessment_date", "misconception_indicator",
        "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_attendance(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse StudentSchoolAttendanceEvents.xml into a flat DataFrame.

    Columns: student_unique_id, school_id, event_date,
             attendance_status, _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "StudentSchoolAttendanceEvents.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for event in root.findall(_tag("StudentSchoolAttendanceEvent")):
        stu_ref = event.find(_tag("StudentReference"))
        sch_ref = event.find(_tag("SchoolReference"))

        rows.append({
            "student_unique_id": _text(stu_ref, "StudentUniqueId") if stu_ref is not None else None,
            "school_id": _text(sch_ref, "SchoolId") if sch_ref is not None else None,
            "event_date": _text(event, "EventDate"),
            "attendance_status": _text(event, "AttendanceEventCategory"),
            **meta,
        })

    schema = _make_schema([
        "student_unique_id", "school_id", "event_date",
        "attendance_status", "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


def parse_standards(spark: SparkSession, input_dir: str) -> DataFrame:
    """Parse LearningStandards.xml into a flat DataFrame.

    Columns: standard_code, standard_description, _source_system, _loaded_at
    """
    filepath = os.path.join(input_dir, "LearningStandards.xml")
    tree = ET.parse(filepath)
    root = tree.getroot()

    meta = _meta_fields()
    rows: List[Dict[str, Any]] = []

    for standard in root.findall(_tag("LearningStandard")):
        rows.append({
            "standard_code": _text(standard, "StandardCode"),
            "standard_description": _text(standard, "StandardDescription"),
            **meta,
        })

    schema = _make_schema([
        "standard_code", "standard_description",
        "_source_system", "_loaded_at",
    ])
    return spark.createDataFrame(rows, schema)


# ---------------------------------------------------------------------------
# run_all: orchestrate parsing and Parquet writes
# ---------------------------------------------------------------------------

_PARSE_REGISTRY = {
    "students": parse_students,
    "schools": parse_schools,
    "staff": parse_staff,
    "sections": parse_sections,
    "enrollments": parse_enrollments,
    "section_associations": parse_section_associations,
    "grades": parse_grades,
    "assessment_results": parse_assessment_results,
    "attendance": parse_attendance,
    "standards": parse_standards,
}


def run_all(
    spark: SparkSession,
    input_dir: str,
    output_dir: str,
) -> Dict[str, DataFrame]:
    """Parse all Ed-Fi XML files and write each as Parquet.

    Parameters
    ----------
    spark : SparkSession
        Active Spark session.
    input_dir : str
        Directory containing the Ed-Fi XML interchange files.
    output_dir : str
        Directory where Parquet files will be written (one sub-directory
        per entity, e.g. ``output_dir/students/``).

    Returns
    -------
    dict[str, DataFrame]
        Mapping from entity name to its parsed DataFrame.
    """
    os.makedirs(output_dir, exist_ok=True)
    results: Dict[str, DataFrame] = {}

    for name, parse_fn in _PARSE_REGISTRY.items():
        df = parse_fn(spark, input_dir)
        parquet_path = os.path.join(output_dir, name)
        df.write.mode("overwrite").parquet(parquet_path)
        results[name] = df

    return results
