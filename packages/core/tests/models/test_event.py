from fustor_event_model.models import EventType, EventBase, InsertEvent, UpdateEvent, DeleteEvent

class TestEventModels:
    def test_event_base(self):
        event = EventBase(fields=["id", "name"], rows=[(1, "test")], event_type=EventType.INSERT, index=123)
        assert event.event_type == EventType.INSERT
        assert event.fields == ["id", "name"]
        assert event.rows == [(1, "test")]
        assert event.index == 123

    def test_insert_event(self):
        event = InsertEvent(event_schema="public", table="users", rows=[{"id": 1, "name": "test"}])
        assert event.event_type == EventType.INSERT
        assert event.event_schema == "public"
        assert event.table == "users"
        assert event.fields == ["id", "name"]

    def test_update_event(self):
        event = UpdateEvent(event_schema="public", table="users", rows=[{"id": 1, "name": "test_updated"}])
        assert event.event_type == EventType.UPDATE
        assert event.table == "users"

    def test_delete_event(self):
        event = DeleteEvent(event_schema="public", table="users", rows=[{"id": 1}])
        assert event.event_type == EventType.DELETE
        assert event.table == "users"
