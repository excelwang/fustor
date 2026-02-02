import httpx
import asyncio
import sys

async def main():
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.post('http://fustor-fusion:8102/api/v1/pipe/session/', json={'task_id': 'test'}, headers={'X-API-Key': 'test-api-key-123'})
            print(f"Status: {resp.status_code}")
            print(f"Body: {resp.text}")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())
