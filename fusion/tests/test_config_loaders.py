# fusion/tests/test_config_loaders.py
"""
Tests for YAML configuration loaders.
"""
import pytest
import tempfile
from pathlib import Path
import yaml

from fustor_fusion.config.validators import validate_url_safe_id
from fustor_fusion.config.datastores import DatastoresConfigLoader
from fustor_fusion.config.views import ViewsConfigLoader, ViewConfig


class TestValidateUrlSafeId:
    """Tests for URL-safe ID validation."""
    
    def test_valid_ids(self):
        """Valid IDs should pass validation."""
        valid_ids = [
            "sync-1",
            "view_fs",
            "research-data-2024",
            "a",
            "1abc",
            "test_view-1",
        ]
        for id_value in valid_ids:
            errors = validate_url_safe_id(id_value)
            assert errors == [], f"Expected '{id_value}' to be valid, got: {errors}"
    
    def test_invalid_ids(self):
        """Invalid IDs should fail validation."""
        invalid_ids = [
            "",           # empty
            "Test",       # uppercase
            "with space", # space
            "hello/world",# slash
            "日本語",      # non-ASCII
            "-starts-with-dash",  # starts with dash
            "_underscore_start",  # starts with underscore
        ]
        for id_value in invalid_ids:
            errors = validate_url_safe_id(id_value)
            assert len(errors) > 0, f"Expected '{id_value}' to be invalid"
    
    def test_id_too_long(self):
        """IDs over 64 characters should fail."""
        long_id = "a" * 65
        errors = validate_url_safe_id(long_id)
        assert any("64 characters" in e for e in errors)


class TestDatastoresConfigLoader:
    """Tests for DatastoresConfigLoader."""
    
    def test_load_valid_config(self, tmp_path):
        """Should load valid datastore config."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "datastores": {
                "test-ds": {
                    "session_timeout_seconds": 60,
                    "api_key": "fk_test123"
                }
            }
        }))
        
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        
        ds = loader.get_datastore("test-ds")
        assert ds is not None
        assert ds.id == "test-ds"
        assert ds.session_timeout_seconds == 60
        assert ds.api_key == "fk_test123"

    def test_load_flat_config(self, tmp_path):
        """Should load flat datastore config (no top-level 'datastores' key)."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "test-ds": {
                "session_timeout_seconds": 60,
                "api_key": "fk_test123"
            }
        }))
        
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        
        ds = loader.get_datastore("test-ds")
        assert ds is not None
        assert ds.id == "test-ds"
        assert ds.session_timeout_seconds == 60
        assert ds.api_key == "fk_test123"
    
    def test_validate_api_key(self, tmp_path):
        """Should validate API key and return datastore_id."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "datastores": {
                "ds1": {"api_key": "key1"},
                "ds2": {"api_key": "key2"}
            }
        }))
        
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        
        assert loader.validate_api_key("key1") == "ds1"
        assert loader.validate_api_key("key2") == "ds2"
        assert loader.validate_api_key("invalid") is None
    
    def test_save_api_key(self, tmp_path):
        """Should save new API key to YAML file."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "datastores": {
                "ds1": {"api_key": "old_key"}
            }
        }))
        
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        
        loader.save_api_key("ds1", "new_key")
        
        # Verify file was updated
        with open(config_file) as f:
            data = yaml.safe_load(f)
        
        # Should respect the existing 'datastores' key if it was present
        assert "datastores" in data
        assert data["datastores"]["ds1"]["api_key"] == "new_key"
        
        # Verify internal state was updated
        assert loader.validate_api_key("new_key") == "ds1"
        assert loader.validate_api_key("old_key") is None

    def test_get_all_ids(self, tmp_path):
        """Should return all registered datastore IDs."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "datastores": {
                "ds1": {"api_key": "key1"},
                "ds2": {"api_key": "key2"}
            }
        }))
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        assert set(loader.get_all_ids()) == {"ds1", "ds2"}

    def test_reload_config(self, tmp_path):
        """Should reload configuration from file."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "datastores": {
                "ds1": {"api_key": "key1"}
            }
        }))
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        assert loader.get_datastore("ds1").api_key == "key1"

        # Modify file and reload
        config_file.write_text(yaml.dump({
            "datastores": {
                "ds1": {"api_key": "key1-updated"}
            }
        }))
        loader.reload()
        assert loader.get_datastore("ds1").api_key == "key1-updated"

    def test_url_unsafe_id_rejected(self, tmp_path):
        """Should reject datastore config with URL-unsafe ID."""
        config_file = tmp_path / "datastores-config.yaml"
        config_file.write_text(yaml.dump({
            "datastores": {
                "Invalid ID": {"api_key": "key1"}
            }
        }))
        
        loader = DatastoresConfigLoader(config_file)
        loader.load()
        
        # Should not load invalid config
        assert len(loader.get_all_ids()) == 0


class TestViewsConfigLoader:
    """Tests for ViewsConfigLoader."""
    
    def test_scan_directory(self, tmp_path):
        """Should scan directory and load all view configs."""
        views_dir = tmp_path / "views-config"
        views_dir.mkdir()
        
        (views_dir / "view-a.yaml").write_text(yaml.dump({
            "id": "view-a",
            "datastore_id": "ds1",
            "driver": "fs"
        }))
        
        (views_dir / "view-b.yaml").write_text(yaml.dump({
            "id": "view-b",
            "datastore_id": "ds2",
            "driver": "fs",
            "disabled": True
        }))
        
        loader = ViewsConfigLoader(views_dir)
        views = loader.scan()
        
        assert len(views) == 2
        assert "view-a" in views
        assert "view-b" in views
    
    def test_get_enabled(self, tmp_path):
        """Should return only enabled views."""
        views_dir = tmp_path / "views-config"
        views_dir.mkdir()
        
        (views_dir / "enabled.yaml").write_text(yaml.dump({
            "id": "enabled",
            "datastore_id": "ds1",
            "driver": "fs"
        }))
        
        (views_dir / "disabled.yaml").write_text(yaml.dump({
            "id": "disabled",
            "datastore_id": "ds1",
            "driver": "fs",
            "disabled": True
        }))
        
        loader = ViewsConfigLoader(views_dir)
        loader.scan()
        
        enabled = loader.get_enabled()
        assert len(enabled) == 1
        assert "enabled" in enabled
    
    def test_get_by_datastore(self, tmp_path):
        """Should filter views by datastore_id."""
        views_dir = tmp_path / "views-config"
        views_dir.mkdir()
        
        (views_dir / "ds1-view.yaml").write_text(yaml.dump({
            "id": "ds1-view",
            "datastore_id": "ds1",
            "driver": "fs"
        }))
        
        (views_dir / "ds2-view.yaml").write_text(yaml.dump({
            "id": "ds2-view",
            "datastore_id": "ds2",
            "driver": "fs"
        }))
        
        loader = ViewsConfigLoader(views_dir)
        loader.scan()
        
        ds1_views = loader.get_by_datastore("ds1")
        assert len(ds1_views) == 1
        assert ds1_views[0].id == "ds1-view"
    
    def test_invalid_id_rejected(self, tmp_path):
        """Should reject view config with invalid ID."""
        views_dir = tmp_path / "views-config"
        views_dir.mkdir()
        
        (views_dir / "invalid.yaml").write_text(yaml.dump({
            "id": "Invalid ID",  # uppercase and space
            "datastore_id": "ds1",
            "driver": "fs"
        }))
        
        loader = ViewsConfigLoader(views_dir)
        views = loader.scan()
        
        # Should not load invalid config
        assert len(views) == 0
