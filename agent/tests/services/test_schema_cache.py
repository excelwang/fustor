import pytest
import os
import json
from unittest.mock import patch, mock_open

from fustor_agent.services import schema_cache

# Mock the SCHEMA_CACHE_DIR for testing purposes
@pytest.fixture(autouse=True)
def mock_schema_cache_dir():
    with patch('fustor_agent.services.schema_cache.SCHEMA_CACHE_DIR', '/tmp/test_schemas'):
        yield

@pytest.fixture
def sample_schema_data():
    return {
        "tables": {
            "users": {
                "columns": [
                    {"name": "id", "type": "int"},
                    {"name": "name", "type": "varchar"}
                ]
            }
        }
    }

class TestSchemaCache:
    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    @patch('os.path.exists', side_effect=[False, True]) # First call for makedirs, second for file_path
    def test_save_source_schema_success(self, mock_exists, mock_file_open, mock_makedirs, sample_schema_data):
        source_id = "test_source"
        schema_cache.save_source_schema(source_id, sample_schema_data)
        
        mock_makedirs.assert_called_once_with('/tmp/test_schemas', exist_ok=True)
        mock_file_open.assert_called_once_with('/tmp/test_schemas/source_test_source.schema.json', 'w', encoding='utf-8')
        # Concatenate all calls to write and assert the final content
        written_content = "".join([call.args[0] for call in mock_file_open().write.call_args_list])
        assert written_content == json.dumps(sample_schema_data, indent=2)

    @patch('os.makedirs', side_effect=OSError("Permission denied"))
    @patch('os.path.exists', return_value=False)
    def test_save_source_schema_makedirs_failure(self, mock_exists, mock_makedirs, sample_schema_data):
        source_id = "test_source"
        with pytest.raises(OSError, match="Permission denied"):
            schema_cache.save_source_schema(source_id, sample_schema_data)

    @patch('os.makedirs')
    @patch('builtins.open', new_callable=mock_open)
    def test_save_source_schema_write_failure(self, mock_file_open, mock_makedirs, sample_schema_data):
        source_id = "test_source"
        mock_file_open.return_value.__enter__.return_value.write.side_effect = IOError("Disk full")
        with pytest.raises(IOError, match="Disk full"):
            schema_cache.save_source_schema(source_id, sample_schema_data)

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_load_source_schema_success(self, mock_file_open, mock_exists, sample_schema_data):
        source_id = "test_source"
        mock_file_open.return_value.__enter__.return_value.read.return_value = json.dumps(sample_schema_data)
        
        loaded_schema = schema_cache.load_source_schema(source_id)
        assert loaded_schema == sample_schema_data
        mock_file_open.assert_called_once_with('/tmp/test_schemas/source_test_source.schema.json', 'r', encoding='utf-8')

    @patch('os.path.exists', return_value=False)
    def test_load_source_schema_file_not_found(self, mock_exists):
        source_id = "non_existent_source"
        loaded_schema = schema_cache.load_source_schema(source_id)
        assert loaded_schema is None

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_load_source_schema_read_failure(self, mock_file_open, mock_exists):
        source_id = "test_source"
        mock_file_open.return_value.__enter__.return_value.read.side_effect = IOError("Read error")
        loaded_schema = schema_cache.load_source_schema(source_id)
        assert loaded_schema is None
    

    @patch('os.path.exists', return_value=True)
    @patch('builtins.open', new_callable=mock_open)
    def test_load_source_schema_json_decode_error(self, mock_file_open, mock_exists):
        source_id = "test_source"
        mock_file_open.return_value.__enter__.return_value.read.return_value = "invalid json"
        loaded_schema = schema_cache.load_source_schema(source_id)
        assert loaded_schema is None

    @patch('os.path.exists')
    def test_is_schema_valid_true(self, mock_exists):
        mock_exists.return_value = True
        source_id = "test_source"
        assert schema_cache.is_schema_valid(source_id) is True
        assert mock_exists.call_count == 2

    @patch('os.path.exists')
    def test_is_schema_valid_false_no_marker(self, mock_exists):
        mock_exists.side_effect = [True, False]
        source_id = "test_source"
        assert schema_cache.is_schema_valid(source_id) is False
        assert mock_exists.call_count == 2

    @patch('os.path.exists')
    def test_is_schema_valid_false_no_schema(self, mock_exists):
        mock_exists.return_value = False
        source_id = "non_existent_source"
        assert schema_cache.is_schema_valid(source_id) is False
        mock_exists.assert_called_once()
