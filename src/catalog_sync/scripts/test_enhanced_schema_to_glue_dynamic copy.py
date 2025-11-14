import pytest
from unittest.mock import Mock, patch, MagicMock, call
import json
import csv
import io

from enhanced_schema_to_glue_dynamic_copy import (
    _infer_type,
    merge_types,
    infer_schema_from_s3_head,
    ensure_landing_table,
    upload_dynamic_job_script,
    ensure_glue_job,
    get_execution_role,
)


class TestInferType:
    def test_empty_string_returns_string(self):
        assert _infer_type("") == "string"

    def test_none_returns_string(self):
        assert _infer_type(None) == "string"

    def test_integer_returns_int(self):
        assert _infer_type("42") == "int"
        assert _infer_type("-10") == "int"

    def test_float_returns_double(self):
        assert _infer_type("3.14") == "double"
        assert _infer_type("-2.5") == "double"

    def test_boolean_returns_string(self):
        assert _infer_type("true") == "string"
        assert _infer_type("false") == "string"

    def test_text_returns_string(self):
        assert _infer_type("hello") == "string"
        assert _infer_type("data123") == "string"


class TestMergeTypes:
    def test_same_types_return_same(self):
        assert merge_types("string", "string") == "string"
        assert merge_types("int", "int") == "int"
        assert merge_types("double", "double") == "double"

    def test_string_with_any_returns_string(self):
        assert merge_types("string", "int") == "string"
        assert merge_types("double", "string") == "string"

    def test_int_and_double_returns_double(self):
        assert merge_types("int", "double") == "double"
        assert merge_types("double", "int") == "double"

    def test_unknown_combination_returns_string(self):
        assert merge_types("int", "string") == "string"


class TestInferSchemaFromS3Head:
    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_csv_inference(self, mock_logging):
        mock_s3 = Mock()
        csv_data = "name,age,salary\nJohn,30,50000\nJane,28,60000"
        mock_body = Mock()
        mock_body.read.side_effect = [csv_data.encode('utf-8'), b'']
        mock_s3.get_object.return_value = {"Body": mock_body}

        result = infer_schema_from_s3_head(mock_s3, "test-bucket", "data.csv")

        assert len(result) == 3
        assert result[0]["Name"] == "name"
        assert result[0]["Type"] == "string"
        assert result[1]["Name"] == "age"
        assert result[2]["Name"] == "salary"

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_json_ndjson_inference(self, mock_logging):
        mock_s3 = Mock()
        json_data = '{"id": 1, "name": "Alice"}\n{"id": 2, "name": "Bob"}'
        mock_body = Mock()
        mock_body.read.side_effect = [json_data.encode('utf-8'), b'']
        mock_s3.get_object.return_value = {"Body": mock_body}

        result = infer_schema_from_s3_head(mock_s3, "test-bucket", "data.json")

        assert len(result) == 2
        names = {col["Name"] for col in result}
        assert names == {"id", "name"}

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_tsv_inference(self, mock_logging):
        mock_s3 = Mock()
        tsv_data = "col1\tcol2\tcol3\nvalue1\t100\tvalue3"
        mock_body = Mock()
        mock_body.read.side_effect = [tsv_data.encode('utf-8'), b'']
        mock_s3.get_object.return_value = {"Body": mock_body}

        result = infer_schema_from_s3_head(mock_s3, "test-bucket", "data.tsv")

        assert len(result) == 3
        assert result[0]["Name"] == "col1"

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_plain_text_inference(self, mock_logging):
        mock_s3 = Mock()
        text_data = "line1\nline2\nline3"
        mock_body = Mock()
        mock_body.read.side_effect = [text_data.encode('utf-8'), b'']
        mock_s3.get_object.return_value = {"Body": mock_body}

        result = infer_schema_from_s3_head(mock_s3, "test-bucket", "data.txt")

        assert len(result) == 1
        assert result[0]["Name"] == "line"
        assert result[0]["Type"] == "string"

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_empty_csv_raises_error(self, mock_logging):
        mock_s3 = Mock()
        mock_body = Mock()
        mock_body.read.side_effect = [b'', b'']
        mock_s3.get_object.return_value = {"Body": mock_body}

        with pytest.raises(ValueError, match="CSV appears empty"):
            infer_schema_from_s3_head(mock_s3, "test-bucket", "empty.csv")


class TestEnsureLandingTable:
    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_table_already_exists(self, mock_logging):
        mock_glue = Mock()
        mock_glue.get_table.return_value = {"Table": {}}
        columns = [{"Name": "col1", "Type": "string"}]

        ensure_landing_table(mock_glue, "db", "table1", "s3://bucket/path", columns)

        mock_glue.get_table.assert_called_once()
        mock_glue.create_table.assert_not_called()

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_table_created(self, mock_logging):
        mock_glue = Mock()
        mock_glue.get_table.side_effect = Exception("EntityNotFoundException")
        mock_glue.exceptions.EntityNotFoundException = Exception
        columns = [{"Name": "col1", "Type": "string"}]

        ensure_landing_table(mock_glue, "db", "table1", "s3://bucket/path", columns)

        mock_glue.create_table.assert_called_once()


class TestUploadDynamicJobScript:
    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_script_uploaded(self, mock_logging):
        mock_s3 = Mock()
        script_code = "print('hello')"

        upload_dynamic_job_script(mock_s3, "test-bucket", "scripts/job.py", script_code)

        mock_s3.put_object.assert_called_once()
        call_kwargs = mock_s3.put_object.call_args[1]
        assert call_kwargs["Bucket"] == "test-bucket"
        assert call_kwargs["Key"] == "scripts/job.py"
        assert call_kwargs["Body"] == script_code.encode("utf-8")


class TestEnsureGlueJob:
    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_job_already_exists(self, mock_logging):
        mock_glue = Mock()
        mock_glue.get_job.return_value = {"Job": {}}

        ensure_glue_job(mock_glue, "job1", "arn:aws:iam::role", "s3://bucket/script.py", "4.0", "s3://temp")

        mock_glue.get_job.assert_called_once()
        mock_glue.create_job.assert_not_called()

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_job_created(self, mock_logging):
        mock_glue = Mock()
        mock_glue.get_job.side_effect = Exception("EntityNotFoundException")
        mock_glue.exceptions.EntityNotFoundException = Exception

        ensure_glue_job(mock_glue, "job1", "arn:aws:iam::role", "s3://bucket/script.py", "4.0", "s3://temp")

        mock_glue.create_job.assert_called_once()


class TestGetExecutionRole:
    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_role_from_config(self, mock_logging):
        mock_sts = Mock()
        cfg = {"aws": {"role_arn": "arn:aws:iam::123456789:role/CustomRole"}}

        result = get_execution_role(cfg, mock_sts)

        assert result == "arn:aws:iam::123456789:role/CustomRole"
        mock_sts.get_caller_identity.assert_not_called()

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_auto_detect_role(self, mock_logging):
        mock_sts = Mock()
        mock_sts.get_caller_identity.return_value = {"Account": "123456789"}
        cfg = {"aws": {}}

        result = get_execution_role(cfg, mock_sts)

        assert result == "arn:aws:iam::123456789:role/AWSGlueServiceRole-DataEngineering"

    @patch('enhanced_schema_to_glue_dynamic_copy.logging')
    def test_auto_detect_fails(self, mock_logging):
        mock_sts = Mock()
        mock_sts.get_caller_identity.side_effect = Exception("Access Denied")
        cfg = {"aws": {}}

        with pytest.raises(Exception):
            get_execution_role(cfg, mock_sts)