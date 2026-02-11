import asyncio
import aiohttp
import time
import uuid
import argparse

async def create_session(session, url, api_key, pipe_id):
    headers = {"X-API-Key": api_key}
    payload = {
        "task_id": f"stress-test-{pipe_id}-{uuid.uuid4()}",
        "client_info": {"type": "stress-test"},
        "session_timeout_seconds": 3600
    }
    async with session.post(f"{url}/api/v1/pipe/session/", json=payload, headers=headers) as resp:
        if resp.status != 200:
            text = await resp.text()
            print(f"Failed to create session for {pipe_id}: {resp.status} {text}")
            return None
        data = await resp.json()
        return data["session_id"]

async def send_events(pipe_id, api_key, url, total_events, batch_size=500):
    async with aiohttp.ClientSession() as session:
        session_id = await create_session(session, url, api_key, pipe_id)
        if not session_id:
            return

        print(f"[{pipe_id}] Session created: {session_id}")
        sent = 0
        start_time = time.time()
        
        headers = {"X-API-Key": api_key}
        
        while sent < total_events:
            events = []
            for _ in range(min(batch_size, total_events - sent)):
                uid = uuid.uuid4()
                filename = f"stress_{pipe_id}_{uid}.txt"
                path = f"/{filename}"
                
                # Following fustor-schema-fs requirements
                row = {
                    "path": path,
                    "file_name": filename,
                    "size": 1024,
                    "modified_time": time.time(),
                    "is_directory": False,
                    "is_atomic_write": True
                }
                
                event = {
                    "event_type": "insert",
                    "event_schema": "fs",
                    "table": "files",
                    "fields": ["path", "file_name", "size", "modified_time", "is_directory", "is_atomic_write"],
                    "rows": [row],
                    "message_source": "realtime"
                }
                events.append(event)
            
            payload = {
                "events": events,
                "source_type": "message",
                "is_end": False
            }
            
            try:
                async with session.post(f"{url}/api/v1/pipe/ingest/{session_id}/events", json=payload, headers=headers) as resp:
                    if resp.status != 200:
                        text = await resp.text()
                        if sent == 0: # Only print first error to avoid noise
                             print(f"[{pipe_id}] Error: {resp.status} {text}")
                        if resp.status == 419 or resp.status == 404:
                             session_id = await create_session(session, url, api_key, pipe_id)
                    else:
                        sent += len(events)
                
                elapsed = time.time() - start_time
                if elapsed > 0:
                    rate = sent / elapsed
                    if sent % (batch_size * 20) == 0:
                        print(f"[{pipe_id}] Sent {sent} events. Current rate: {rate:.2f} events/s")
            except Exception as e:
                print(f"[{pipe_id}] Connection error: {e}")
                await asyncio.sleep(0.1)

        total_time = time.time() - start_time
        print(f"[{pipe_id}] Finished. Total sent: {sent}, Total time: {total_time:.2f}s, Avg rate: {sent/total_time:.2f} events/s")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="http://localhost:18890")
    parser.add_argument("--count", type=int, default=100000)
    parser.add_argument("--batch", type=int, default=500)
    args = parser.parse_args()

    t1 = send_events("pipe-1", "agent-1-push-key", args.url, args.count, args.batch)
    t2 = send_events("pipe-2", "agent-2-push-key", args.url, args.count, args.batch)

    await asyncio.gather(t1, t2)

if __name__ == "__main__":
    asyncio.run(main())
