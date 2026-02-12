import pytest
import asyncio
from fustor_source_fs.driver import FSDriver
from fustor_core.models.config import SourceConfig

class TestFSDriverReload:
    """
    Verify Option A: FSDriver Hot Reload via close() + recreate.
    """

    @pytest.mark.asyncio
    async def test_driver_reload(self, tmp_path):
        uri = str(tmp_path)
        config = SourceConfig(id="fs-test", driver="fs", uri=uri)
        
        # 1. Create first instance
        driver1 = FSDriver("fs-test", config)
        assert driver1.uri == uri
        assert driver1._initialized
        
        # Verify singleton behavior
        driver1_dup = FSDriver("fs-test-dup", config)
        assert driver1_dup is driver1
        
        # 2. Close the driver (Trigger Option A reload logic)
        await driver1.close()
        
        # 3. Create second instance (Should be fresh)
        driver2 = FSDriver("fs-test-new", config)
        
        # Verify it's a NEW instance
        assert driver2 is not driver1
        assert driver2._initialized
        
        # Verify singleton cache now holds the new one
        driver2_dup = FSDriver("fs-test-new-dup", config)
        assert driver2_dup is driver2
        
        await driver2.close()
