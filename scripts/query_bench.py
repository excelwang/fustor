import asyncio
import aiohttp
import time
import uuid
import argparse
import statistics

async def create_known_files(url, api_key, pipe_id, count=1000):
    async with aiohttp.ClientSession() as session:
        headers = {"X-API-Key": api_key}
        payload = {
            "task_id": f"setup-query-{uuid.uuid4()}",
            "client_info": {"type": "setup"},
            "session_timeout_seconds": 3600
        }
        async with session.post(f"{url}/api/v1/pipe/session/", json=payload, headers=headers) as resp:
            data = await resp.json()
            session_id = data["session_id"]
        
        paths = []
        events = []
        for i in range(count):
            path = f"/known_file_{i}.txt"
            paths.append(path)
            events.append({
                "event_type": "insert",
                "event_schema": "fs",
                "table": "files",
                "fields": ["path", "file_name", "size", "modified_time", "is_directory"],
                "rows": [{"path": path, "file_name": f"known_file_{i}.txt", "size": 1024, "modified_time": time.time(), "is_directory": False}],
            })
        
        # Batch send
        for i in range(0, len(events), 100):
            batch = {"events": events[i:i+100], "source_type": "message"}
            async with session.post(f"{url}/api/v1/pipe/ingest/{session_id}/events", json=batch, headers=headers) as resp:
                await resp.read()
        
        return paths

async def run_query_bench(url, api_key, paths, duration=10):
    async with aiohttp.ClientSession() as session:
        headers = {"X-API-Key": api_key}
        latencies = []
        start_time = time.time()
        end_time = start_time + duration
        queries = 0
        
        while time.time() < end_time:
            path = paths[queries % len(paths)]
            q_start = time.perf_counter()
            try:
                async with session.get(f"{url}/api/v1/views/unified-view/metadata?path={path}", headers=headers) as resp:
                    await resp.read()
                    latencies.append(time.perf_counter() - q_start)
                    queries += 1
            except Exception as e:
                print(f"Query error: {e}")
                await asyncio.sleep(0.01)
        
        total_duration = time.time() - start_time
        if not latencies:
            return {"queries": 0, "avg_ms": 0, "p95_ms": 0, "throughput": 0}
            
        avg_lat = statistics.mean(latencies) * 1000
        p95_lat = statistics.quantiles(latencies, n=20)[18] * 1000
        throughput = queries / total_duration
        
        return {
            "queries": queries,
            "avg_ms": avg_lat,
            "p95_ms": p95_lat,
            "throughput": throughput
        }

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--url-api", default="http://localhost:8103")
    parser.add_argument("--url-push", default="http://localhost:18890")
    parser.add_argument("--mode", choices=["setup", "bench"], default="bench")
    args = parser.parse_args()

    if args.mode == "setup":
        paths = await create_known_files(args.url_push, "agent-1-push-key", "pipe-1")
        with open("known_paths.txt", "w") as f:
            for p in paths:
                f.write(p + "\n")
        print(f"Setup complete. 1000 paths saved.")
    else:
        with open("known_paths.txt", "r") as f:
            paths = [line.strip() for line in f]
        
        print("Running benchmark...")
        result = await run_query_bench(args.url_api, "external-read-only-key", paths)
        print(f"Queries: {result['queries']}")
        print(f"Throughput: {result['throughput']:.2f} req/s")
        print(f"Avg Latency: {result['avg_ms']:.2f} ms")
        print(f"P95 Latency: {result['p95_ms']:.2f} ms")

if __name__ == "__main__":
    asyncio.run(main())
