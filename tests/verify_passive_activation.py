import sys
import os
from pathlib import Path
from unittest.mock import MagicMock

# Mock dotenv before it's imported by fustor_agent
sys.modules['dotenv'] = MagicMock()

# Add project roots to sys.path
import sys
project_root = Path(__file__).parent.parent
sys.path.append(str(project_root / "core/src"))
sys.path.append(str(project_root / "agent/src"))
sys.path.append(str(project_root / "fusion/src"))

from fustor_agent.config.unified import AgentPipeConfig, AgentConfigLoader
from fustor_fusion.config.unified import FusionPipeConfig, FusionConfigLoader, ReceiverConfig, ViewConfig
from fustor_core.models.config import SourceConfig, SenderConfig

def test_agent_passive_activation():
    print("Testing Agent passive activation...")
    loader = AgentConfigLoader(config_dir="/tmp/fustor-test-agent")
    os.makedirs("/tmp/fustor-test-agent", exist_ok=True)
    
    with open("/tmp/fustor-test-agent/default.yaml", "w") as f:
        f.write("""
sources:
  s1: {driver: fs, uri: "file:///tmp", disabled: false}
  s2: {driver: fs, uri: "file:///tmp", disabled: true}
pipes:
  p1: {source: s1, sender: send1}
  p2: {source: s2, sender: send1}
senders:
  send1: {driver: fusion, uri: "http://localhost:8102"}
""")
    
    loader.load_all()
    enabled = loader.get_enabled_pipes()
    
    assert "p1" in enabled, "p1 should be enabled because s1 is enabled"
    assert "p2" not in enabled, "p2 should be disabled because s2 is disabled"
    print("✓ Agent passive activation logic verified.")

def test_fusion_passive_activation():
    print("Testing Fusion passive activation...")
    loader = FusionConfigLoader(config_dir="/tmp/fustor-test-fusion")
    os.makedirs("/tmp/fustor-test-fusion", exist_ok=True)
    
    with open("/tmp/fustor-test-fusion/default.yaml", "w") as f:
        f.write("""
receivers:
  r1: {driver: http, port: 8102, disabled: false}
  r2: {driver: http, port: 8103, disabled: true}
views:
  v1: {driver: fs, disabled: false}
  v2: {driver: fs, disabled: true}
pipes:
  p1: {receiver: r1, views: [v1]}
  p2: {receiver: r1, views: [v2]}
  p3: {receiver: r2, views: [v1]}
  p4: {receiver: r1, views: [v1, v2]}
""")
    
    loader.load_all()
    enabled = loader.get_enabled_pipes()
    
    assert "p1" in enabled, "p1 should be enabled (r1, v1 both enabled)"
    assert "p2" not in enabled, "p2 should be disabled (v2 disabled)"
    assert "p3" not in enabled, "p3 should be disabled (r2 disabled)"
    assert "p4" in enabled, "p4 should be enabled (r1 enabled, v1 enabled)"
    print("✓ Fusion passive activation logic verified.")

if __name__ == "__main__":
    try:
        test_agent_passive_activation()
        test_fusion_passive_activation()
        print("\nALL PASSIVE ACTIVATION TESTS PASSED")
    except Exception as e:
        print(f"\nTEST FAILED: {e}")
        sys.exit(1)
    finally:
        import shutil
        shutil.rmtree("/tmp/fustor-test-agent", ignore_errors=True)
        shutil.rmtree("/tmp/fustor-test-fusion", ignore_errors=True)
