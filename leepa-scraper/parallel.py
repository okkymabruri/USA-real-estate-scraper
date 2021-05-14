import asyncio
from enum import Enum


class Status(Enum):
    OK = 1
    ERROR = 2
    IGNORE = 3


class TaskResult:
    def __init__(self, query, result, status: Status):
        self.query = query
        self.result = result
        self.status = status


async def download(code):
    wait_time = code % 3 + 1
    await asyncio.sleep(wait_time)
    if code == 2:
        return TaskResult(code, "_" + str(code), Status.ERROR)
    return TaskResult(code, "_" + str(code), Status.OK)


async def parallelize(queries, process, on_success, max_workers, loop):
    dltasks = set()
    while len(queries) > 0 or len(dltasks) > 0:
        _done = []
        if len(queries) == 0:
            _done, dltasks = await asyncio.wait(dltasks)
        if len(dltasks) >= max_workers:
            _done, dltasks = await asyncio.wait(
                dltasks, return_when=asyncio.FIRST_COMPLETED
            )
        for task in _done:
            res = task.result()
            if res.status == Status.OK:
                on_success(res.query, res.result)
            if res.status == Status.ERROR:
                print("ERROR", res.query)
                queries.append(res.query)
            if res.status == Status.IGNORE:
                print("IGNORE", res.query)
        if len(queries) > 0:
            query = queries.pop(0)
            dltasks.add(loop.create_task(process(query)))


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    total_future = asyncio.ensure_future(
        parallelize([i for i in range(10)], download, print, 10, loop)
    )
    loop.run_until_complete(total_future)
