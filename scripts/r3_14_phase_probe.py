"""Optional low-volume timing hooks for R3-14 summary diagnosis."""

from __future__ import annotations

import contextvars
import threading
import time

import takler.server.grpc_transport as grpc_module
import takler.server.http_transport as http_module
import takler.server.query_service as query_module


class SummaryPhaseProbe:
    def __init__(self, service, grpc_service):
        self.service = service
        self.grpc_service = grpc_service
        self.events: list[dict] = []
        self._capture_local = threading.local()
        self._page_event = contextvars.ContextVar(
            "r3_14_summary_page_event", default=None
        )
        self._rpc_event = contextvars.ContextVar(
            "r3_14_summary_rpc_event", default=None
        )
        self._original_capture = service._capture
        self._original_page = service.page
        self._original_grpc_query = grpc_service._query
        self._original_grpc_decode = grpc_module.query_from_pb2
        self._original_encode = query_module.encode_query_document
        self._original_decode = query_module.decode_query_document
        self._original_grpc_encode = grpc_module.query_to_pb2
        self._original_http_encode = http_module.encode_query_document

    def install(self):
        def capture(*args):
            event = {
                "phase": "summary_capture",
                "snapshot_id": args[3],
                "start_ns": time.perf_counter_ns(),
                "thread_cpu_start_ns": time.thread_time_ns(),
                "encode_ns": 0,
                "encoded_bytes": 0,
                "pages": 0,
            }
            self._capture_local.event = event
            try:
                return self._original_capture(*args)
            finally:
                event["end_ns"] = time.perf_counter_ns()
                event["thread_cpu_ns"] = time.thread_time_ns() - event.pop(
                    "thread_cpu_start_ns"
                )
                self.events.append(event)
                self._capture_local.event = None

        def encode(document):
            active = getattr(self._capture_local, "event", None)
            if active is None or document.kind != "page":
                return self._original_encode(document)
            started = time.perf_counter_ns()
            raw = self._original_encode(document)
            active["encode_ns"] += time.perf_counter_ns() - started
            active["encoded_bytes"] += len(raw)
            active["pages"] += 1
            return raw

        def decode(raw, *, expected=None):
            active = self._page_event.get()
            if active is None:
                return self._original_decode(raw, expected=expected)
            started = time.perf_counter_ns()
            reply = self._original_decode(raw, expected=expected)
            active["decode_ns"] += time.perf_counter_ns() - started
            return reply

        async def page(request):
            event = {
                "phase": "summary_page",
                "start_ns": time.perf_counter_ns(),
                "decode_ns": 0,
                "initial": request.cursor is None,
            }
            token = self._page_event.set(event)
            try:
                reply = await self._original_page(request)
                event["snapshot_id"] = getattr(reply, "snapshot_id", None)
                event["page_index"] = getattr(reply, "page_index", None)
                event["error_code"] = getattr(reply, "code", None)
                rpc_event = self._rpc_event.get()
                if rpc_event is not None:
                    rpc_event["snapshot_id"] = event["snapshot_id"]
                    rpc_event["page_index"] = event["page_index"]
                return reply
            finally:
                event["end_ns"] = time.perf_counter_ns()
                self.events.append(event)
                self._page_event.reset(token)

        async def grpc_query(request, expected):
            if expected != "page_request":
                return await self._original_grpc_query(request, expected)
            event = {
                "phase": "summary_grpc_rpc",
                "start_ns": time.perf_counter_ns(),
                "request_decode_ns": 0,
            }
            token = self._rpc_event.set(event)
            try:
                return await self._original_grpc_query(request, expected)
            finally:
                event["end_ns"] = time.perf_counter_ns()
                self.events.append(event)
                self._rpc_event.reset(token)

        def grpc_decode(message, *, expected=None):
            active = self._rpc_event.get()
            if active is None:
                return self._original_grpc_decode(message, expected=expected)
            started = time.perf_counter_ns()
            reply = self._original_grpc_decode(message, expected=expected)
            active["request_decode_ns"] += time.perf_counter_ns() - started
            return reply

        def grpc_encode(document):
            if document.kind != "page":
                return self._original_grpc_encode(document)
            started = time.perf_counter_ns()
            encoded = self._original_grpc_encode(document)
            self.events.append(
                {
                    "phase": "summary_wire_encode",
                    "transport": "grpc",
                    "snapshot_id": document.snapshot_id,
                    "page_index": document.page_index,
                    "start_ns": started,
                    "end_ns": time.perf_counter_ns(),
                    "bytes": len(encoded.json),
                }
            )
            return encoded

        def http_encode(document):
            if document.kind != "page":
                return self._original_http_encode(document)
            started = time.perf_counter_ns()
            encoded = self._original_http_encode(document)
            self.events.append(
                {
                    "phase": "summary_wire_encode",
                    "transport": "http",
                    "snapshot_id": document.snapshot_id,
                    "page_index": document.page_index,
                    "start_ns": started,
                    "end_ns": time.perf_counter_ns(),
                    "bytes": len(encoded),
                }
            )
            return encoded

        self.service._capture = capture
        self.service.page = page
        self.grpc_service._query = grpc_query
        query_module.encode_query_document = encode
        query_module.decode_query_document = decode
        grpc_module.query_from_pb2 = grpc_decode
        grpc_module.query_to_pb2 = grpc_encode
        http_module.encode_query_document = http_encode
        return self

    def close(self):
        self.service._capture = self._original_capture
        self.service.page = self._original_page
        self.grpc_service._query = self._original_grpc_query
        query_module.encode_query_document = self._original_encode
        query_module.decode_query_document = self._original_decode
        grpc_module.query_from_pb2 = self._original_grpc_decode
        grpc_module.query_to_pb2 = self._original_grpc_encode
        http_module.encode_query_document = self._original_http_encode
