import pytest
from fustor_core.models.states import EventBusState, PipelineState, EventBusInstance, PipelineInstanceDTO

def test_event_bus_state_enum():
    assert EventBusState.IDLE.name == "IDLE"
    assert EventBusState.PRODUCING.name == "PRODUCING"
    assert EventBusState.ERROR.name == "ERROR"

def test_pipeline_state_enum():
    assert PipelineState.STOPPED.name == "STOPPED"
    # --- REFACTORED: Test for new two-phase states instead of obsolete RUNNING state ---
    assert PipelineState.SNAPSHOT_SYNC.name == "SNAPSHOT_SYNC"
    assert PipelineState.MESSAGE_SYNC.name == "MESSAGE_SYNC"
    # --- END REFACTOR ---
    assert PipelineState.RUNNING_CONF_OUTDATE.name == "RUNNING_CONF_OUTDATE"
    assert PipelineState.STOPPING.name == "STOPPING"
    assert PipelineState.ERROR.name == "ERROR"

def test_event_bus_instance_dto():
    dto = EventBusInstance(
        id="bus-123",
        source_name="my-source",
        state=EventBusState.PRODUCING,
        info="Bus is actively producing events.",
        statistics={"events_produced": 100, "consumers": 2}
    )
    assert dto.id == "bus-123"
    assert dto.source_name == "my-source"
    assert dto.state == EventBusState.PRODUCING
    assert dto.info == "Bus is actively producing events."
    assert dto.statistics == {"events_produced": 100, "consumers": 2}

def test_pipeline_instance_dto():
    bus_dto = EventBusInstance(
        id="bus-456",
        source_name="another-source",
        state=EventBusState.IDLE,
        info="Bus is idle.",
        statistics={}
    )
    # --- REFACTORED: Use one of the new valid states for the test ---
    dto = PipelineInstanceDTO(
        id="pipeline-abc",
        state=PipelineState.MESSAGE_SYNC,
        info="Pipeline task is running normally.",
        bus_info=bus_dto,
        bus_id="bus-456",
        statistics={"events_pushed": 50, "last_event_id": "xyz"}
    )
    # --- END REFACTOR ---
    assert dto.id == "pipeline-abc"
    assert dto.state == PipelineState.MESSAGE_SYNC
    assert dto.info == "Pipeline task is running normally."
    assert dto.bus_info == bus_dto
    assert dto.bus_id == "bus-456"
    assert dto.statistics == {"events_pushed": 50, "last_event_id": "xyz"}

def test_pipeline_instance_dto_no_bus_info():
    dto = PipelineInstanceDTO(
        id="pipeline-def",
        state=PipelineState.STOPPED,
        info="Pipeline task is stopped.",
        bus_info=None,
        bus_id=None,
        statistics={}
    )
    assert dto.id == "pipeline-def"
    assert dto.state == PipelineState.STOPPED
    assert dto.info == "Pipeline task is stopped."
    assert dto.bus_info is None
    assert dto.bus_id is None
    assert dto.statistics == {}