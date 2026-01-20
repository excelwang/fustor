from fastapi import APIRouter, Depends, Response
from typing import Dict, Any

from fustor_agent.services.instances.bus import EventBusService
from fustor_agent.services.instances.sync import SyncInstanceService
from fustor_agent.api.dependencies import get_app

router = APIRouter()

@router.get("/metrics", summary="Get Prometheus metrics", response_class=Response)
async def get_metrics(
    app = Depends(get_app)
):
    """
    Returns application metrics in Prometheus exposition format.
    """
    bus_service: EventBusService = app.bus_service
    sync_instance_service: SyncInstanceService = app.sync_instance_service

    metrics_output = []

    # Process Bus metrics
    for bus_id, bus_instance in bus_service.list_instances().items():
        bus_metrics = bus_instance.get_dto().statistics
        
        # fustor_agent_events_produced_total
        metrics_output.append(
            f'# HELP fustor_agent_events_produced_total Total number of events produced by source bus.\n'
            f'# TYPE fustor_agent_events_produced_total counter'
        )
        metrics_output.append(
            f'fustor_agent_events_produced_total{{source_id="{bus_instance.source_id}",bus_id="{bus_id}"}} {bus_metrics.get("events_produced_total", 0)}'
        )

        # fustor_agent_bus_queue_size
        metrics_output.append(
            f'# HELP fustor_agent_bus_queue_size Current number of events in the bus queue.\n'
            f'# TYPE fustor_agent_bus_queue_size gauge'
        )
        metrics_output.append(
            f'fustor_agent_bus_queue_size{{source_id="{bus_instance.source_id}",bus_id="{bus_id}"}} {bus_metrics.get("bus_queue_size", 0)}'
        )

    # Process Sync metrics
    for sync_id, sync_instance in sync_instance_service.list_instances().items():
        sync_metrics = sync_instance.get_dto().statistics

        # fustor_agent_events_pushed_total
        metrics_output.append(
            f'# HELP fustor_agent_events_pushed_total Total number of events successfully pushed to pusher.\n'
            f'# TYPE fustor_agent_events_pushed_total counter'
        )
        metrics_output.append(
            f'fustor_agent_events_pushed_total{{sync_id="{sync_id}",source_id="{sync_instance.config.source}",pusher_id="{sync_instance.config.pusher}"}} {sync_metrics.get("events_pushed_total", 0)}'
        )

        # fustor_agent_sync_push_latency_seconds
        # For a histogram, we'll just expose the sum and count for simplicity without a library
        # In a real scenario, you'd calculate buckets, but for a no-dependency approach, sum/count is a start.
        latencies = sync_metrics.get("sync_push_latency_seconds", [])
        if latencies:
            total_latency = sum(latencies)
            count_latency = len(latencies)
            metrics_output.append(
                f'# HELP fustor_agent_sync_push_latency_seconds Latency of pushing event batches to pushers.\n'
                f'# TYPE fustor_agent_sync_push_latency_seconds summary' # Using summary type for sum/count
            )
            metrics_output.append(
                f'fustor_agent_sync_push_latency_seconds_sum{{sync_id="{sync_id}",source_id="{sync_instance.config.source}",pusher_id="{sync_instance.config.pusher}"}} {total_latency:g}'
            )
            metrics_output.append(
                f'fustor_agent_sync_push_latency_seconds_count{{sync_id="{sync_id}",source_id="{sync_instance.config.source}",pusher_id="{sync_instance.config.pusher}"}} {count_latency}'
            )

    return Response(content="\n".join(metrics_output), media_type="text/plain; version=0.0.4")
