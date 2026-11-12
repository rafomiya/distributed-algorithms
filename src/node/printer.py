"""Impressora simulada independente: aceita trabalho e permite consultar por ID.

Não agenda pedidos nem concede permissões. Colisão retorna 409 e é uma
violação observável nos testes, mesmo que o dispositivo recuse a impressão.
"""
import asyncio
import os
import socket
import time

from aiohttp import web

from .common import EventLog


class Printer:
    def __init__(self):
        self.id = os.environ.get("NODE_ID", socket.gethostname())
        self.page_seconds = float(os.environ.get("PAGE_SECONDS", "0.15"))
        self.jobs = {}
        self.active = None
        self.tasks = set()
        self.overlap_attempts = 0
        self.log = EventLog(self.id, os.environ.get("LOG_DIR", "/data/logs"))

    async def h_print(self, req):
        try:
            payload = await req.json()
            job_id = payload["job_id"]
            if not isinstance(job_id, str) or not job_id:
                raise ValueError("job_id inválido")
            if payload["printer"] != self.id:
                raise ValueError("impressora incorreta")
            pages = payload["pages"]
            if type(pages) is not int or not 1 <= pages <= 100:
                raise ValueError("pages inválido")
            record = {key: payload[key] for key in (
                "job_id", "computer", "printer", "document", "pages")}
        except (KeyError, TypeError, ValueError) as error:
            raise web.HTTPBadRequest(text=str(error)) from error
        if job_id in self.jobs:
            existing = self.jobs[job_id]
            if any(existing[key] != value for key, value in record.items()):
                raise web.HTTPConflict(text="job_id reutilizado com outro conteúdo")
            return web.json_response(existing)
        if self.active is not None:
            self.overlap_attempts += 1
            self.log("overlap_violation", active=self.active, rejected=job_id)
            raise web.HTTPConflict(text="violação da exclusão mútua: impressora ocupada")
        record.update(status="printing", started_at=time.time(),
                      started_mono_ns=time.monotonic_ns())
        self.jobs[job_id] = record
        self.active = job_id
        self.log("print_started", **record)
        task = asyncio.create_task(self._complete(record))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        return web.json_response(dict(record), status=202)

    async def _complete(self, record):
        await asyncio.sleep(record["pages"] * self.page_seconds)
        record.update(status="completed", completed_at=time.time(),
                      completed_mono_ns=time.monotonic_ns())
        self.active = None
        self.log("print_completed", **record)

    async def h_health(self, req):
        return web.json_response({"ok": True, "role": "printer", "node": self.id})

    async def h_state(self, req):
        return web.json_response({"node": self.id, "active": self.active,
            "overlap_attempts": self.overlap_attempts, "jobs": list(self.jobs.values())})

    async def stop(self, app):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.log.close()

    def app(self):
        app = web.Application()
        app.add_routes([web.post("/print", self.h_print),
            web.get("/health", self.h_health), web.get("/state", self.h_state)])
        app.on_cleanup.append(self.stop)
        return app


if __name__ == "__main__":
    web.run_app(Printer().app(), host="0.0.0.0",
                port=int(os.environ.get("PORT", "8000")), access_log=None)
