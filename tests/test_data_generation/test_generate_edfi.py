"""Tests for Ed-Fi XML data generator."""

import os

import pytest
from lxml import etree

from data_generation.generate_edfi_xml import generate_edfi_district

NS = "http://ed-fi.org/0220"


@pytest.fixture(scope="module")
def edfi_output(tmp_path_factory):
    """Generate a small Ed-Fi dataset once for all tests in this module."""
    output_dir = str(tmp_path_factory.mktemp("edfi_xml"))
    generate_edfi_district(output_dir, num_students=75)
    return output_dir


EXPECTED_FILES = [
    "Students.xml",
    "Schools.xml",
    "Staff.xml",
    "Sections.xml",
    "StudentSchoolAssociations.xml",
    "StudentSectionAssociations.xml",
    "Grades.xml",
    "StudentAssessments.xml",
    "StudentSchoolAttendanceEvents.xml",
    "LearningStandards.xml",
]


class TestGeneratesAllRequiredFiles:
    def test_generates_all_required_files(self, edfi_output):
        """All 10 XML files must exist in the output directory."""
        generated = os.listdir(edfi_output)
        for filename in EXPECTED_FILES:
            assert filename in generated, f"Missing expected file: {filename}"

    def test_files_are_valid_xml(self, edfi_output):
        """Every generated file must be well-formed XML."""
        for filename in EXPECTED_FILES:
            filepath = os.path.join(edfi_output, filename)
            tree = etree.parse(filepath)
            assert tree.getroot() is not None


class TestStudentsXml:
    def test_students_xml_has_correct_structure(self, edfi_output):
        """Student records must contain required fields."""
        tree = etree.parse(os.path.join(edfi_output, "Students.xml"))
        root = tree.getroot()
        students = root.findall(f"{{{NS}}}Student")
        assert len(students) >= 70, (
            f"Expected at least 70 students, got {len(students)}"
        )

        # Check that most students have required fields
        valid_count = 0
        for student in students:
            uid = student.find(f"{{{NS}}}StudentUniqueId")
            first = student.find(f"{{{NS}}}FirstName")
            last = student.find(f"{{{NS}}}LastSurname")
            birth = student.find(f"{{{NS}}}BirthDate")
            email = student.find(f"{{{NS}}}Email")
            if (
                uid is not None
                and uid.text
                and first is not None
                and last is not None
                and birth is not None
                and email is not None
            ):
                valid_count += 1
        # At least 95% should be fully valid (2% planted with null id)
        assert valid_count >= len(students) * 0.95

    def test_students_have_null_ids_planted(self, edfi_output):
        """Approximately 2% of students should have null/empty StudentUniqueId."""
        tree = etree.parse(os.path.join(edfi_output, "Students.xml"))
        root = tree.getroot()
        students = root.findall(f"{{{NS}}}Student")
        null_id_count = 0
        for student in students:
            uid = student.find(f"{{{NS}}}StudentUniqueId")
            if uid is None or not uid.text or uid.text.strip() == "":
                null_id_count += 1
        assert null_id_count >= 1, "Expected at least 1 student with null StudentUniqueId"


class TestPlantedDqIssues:
    def test_planted_dq_issues_exist(self, edfi_output):
        """Invalid SchoolIDs must be present in StudentSchoolAssociations."""
        tree = etree.parse(
            os.path.join(edfi_output, "StudentSchoolAssociations.xml")
        )
        root = tree.getroot()
        assocs = root.findall(f"{{{NS}}}StudentSchoolAssociation")
        assert len(assocs) > 0, "No StudentSchoolAssociation records found"

        invalid_school_ids = []
        for assoc in assocs:
            school_ref = assoc.find(f"{{{NS}}}SchoolReference")
            if school_ref is not None:
                school_id = school_ref.find(f"{{{NS}}}SchoolId")
                if school_id is not None and school_id.text and "INVALID" in school_id.text:
                    invalid_school_ids.append(school_id.text)

        assert len(invalid_school_ids) >= 1, (
            "Expected at least 1 invalid SchoolId in StudentSchoolAssociations"
        )

    def test_future_entry_dates_planted(self, edfi_output):
        """Some records should have future EntryDate values."""
        tree = etree.parse(
            os.path.join(edfi_output, "StudentSchoolAssociations.xml")
        )
        root = tree.getroot()
        assocs = root.findall(f"{{{NS}}}StudentSchoolAssociation")
        future_count = 0
        for assoc in assocs:
            entry_date = assoc.find(f"{{{NS}}}EntryDate")
            if entry_date is not None and entry_date.text:
                if entry_date.text >= "2027-01-01":
                    future_count += 1
        assert future_count >= 1, "Expected at least 1 future EntryDate"

    def test_dangling_section_references_planted(self, edfi_output):
        """Some StudentSectionAssociations should reference non-existent sections."""
        # Collect valid section IDs
        sec_tree = etree.parse(os.path.join(edfi_output, "Sections.xml"))
        sec_root = sec_tree.getroot()
        valid_section_ids = set()
        for section in sec_root.findall(f"{{{NS}}}Section"):
            sid = section.find(f"{{{NS}}}SectionIdentifier")
            if sid is not None and sid.text:
                valid_section_ids.add(sid.text)

        # Check student-section associations for dangling references
        ssa_tree = etree.parse(
            os.path.join(edfi_output, "StudentSectionAssociations.xml")
        )
        ssa_root = ssa_tree.getroot()
        dangling = 0
        for assoc in ssa_root.findall(f"{{{NS}}}StudentSectionAssociation"):
            sec_ref = assoc.find(f"{{{NS}}}SectionReference")
            if sec_ref is not None:
                sid = sec_ref.find(f"{{{NS}}}SectionIdentifier")
                if sid is not None and sid.text and sid.text not in valid_section_ids:
                    dangling += 1
        assert dangling >= 1, "Expected at least 1 dangling SectionIdentifier"


class TestAssessmentMisconceptions:
    def test_assessment_responses_have_misconceptions(self, edfi_output):
        """Item-level assessment responses must include misconception indicators."""
        tree = etree.parse(
            os.path.join(edfi_output, "StudentAssessments.xml")
        )
        root = tree.getroot()
        assessments = root.findall(f"{{{NS}}}StudentAssessment")
        assert len(assessments) > 0, "No StudentAssessment records found"

        misconception_count = 0
        for assessment in assessments:
            items = assessment.findall(f"{{{NS}}}StudentAssessmentItem")
            for item in items:
                indicator = item.find(f"{{{NS}}}MisconceptionIndicator")
                if indicator is not None and indicator.text:
                    misconception_count += 1

        assert misconception_count >= 3, (
            f"Expected at least 3 misconception indicators, got {misconception_count}"
        )

    def test_assessment_items_have_correct_answer_and_student_answer(self, edfi_output):
        """Each assessment item should have correct_answer and student_answer."""
        tree = etree.parse(
            os.path.join(edfi_output, "StudentAssessments.xml")
        )
        root = tree.getroot()
        assessments = root.findall(f"{{{NS}}}StudentAssessment")

        items_checked = 0
        for assessment in assessments[:5]:  # check first 5 assessments
            items = assessment.findall(f"{{{NS}}}StudentAssessmentItem")
            for item in items:
                correct = item.find(f"{{{NS}}}CorrectAnswer")
                student = item.find(f"{{{NS}}}StudentAnswer")
                assert correct is not None, "Missing CorrectAnswer in item"
                assert student is not None, "Missing StudentAnswer in item"
                items_checked += 1
        assert items_checked >= 1, "No assessment items found to check"
