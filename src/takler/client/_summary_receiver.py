"""Optional native node receiver; Python still validates every page header.

The native module validates the actual nodes, while its private header uses a
root witness solely for PageReply's nonempty-page rule. The public result is
always made of the original Python QueryNode and QuerySnapshot objects.
"""

import sys

from takler.exceptions import QueryResponseError, ServerResponseError
from takler.protocol import query_v1 as wire

try:
    import takler_summary_kernel as _native
except (ImportError, OSError):
    _native = None

_PAGE = wire.PageReply
_PAGE_VALIDATOR = _PAGE.__pydantic_validator__
_NODE_VALIDATOR = wire.SummaryNode.__pydantic_validator__
_DECODE = wire.decode_query_document
_STRICT_JSON = wire.strict_json
_VALID_PATH = wire.valid_path
_KINDS = frozenset(wire.KINDS)
_STATUSES = frozenset(wire.STATUSES)


def create_receiver(transport, scope, depth, old_size, limit, node_class, page_class):
    if _native is None or getattr(_native, "API_VERSION", None) != 1:
        return None
    query = getattr(transport, "query_summary", None)
    # Built-in transports opt out when their ordinary query has been replaced.
    # Custom transports may explicitly implement the callback contract instead.
    original_query = getattr(query, "_original_query", None)
    if (
        not callable(query)
        or (
            original_query is not None
            and getattr(transport.query, "__func__", None) is not original_query
        )
        or page_class is not _PAGE
        or wire.MODEL_BY_KIND.get("page") is not _PAGE
        or _PAGE.__pydantic_validator__ is not _PAGE_VALIDATOR
        or wire.SummaryNode.__pydantic_validator__ is not _NODE_VALIDATOR
        or wire.decode_query_document is not _DECODE
        or wire.strict_json is not _STRICT_JSON
        or wire.valid_path is not _VALID_PATH
        or wire.KINDS != _KINDS
        or wire.STATUSES != _STATUSES
        or wire.MAX_PAGE_BYTES != 262_144
        or wire.MAX_PAGE_NODES != 2048
        or (depth is not None and depth > sys.maxsize)
        or not 0 <= old_size <= sys.maxsize
        or not 0 <= limit <= sys.maxsize
    ):
        return None
    return _Receiver(query, scope, depth, old_size, limit, node_class)


class _Receiver:
    def __init__(self, query, scope, depth, old_size, limit, node_class):
        self.query_summary = query
        self.native = _native
        self.kernel = self.native.Kernel(scope, depth, old_size, limit, node_class)

    def decode(self, raw):
        try:
            return _PAGE.model_validate(self.kernel.prepare(raw))
        except ValueError:
            # Unsupported/noncanonical native input must pass the original
            # strict decoder. Never treat native rejection as accepted input.
            reply = wire.decode_query_document(raw)
            if isinstance(reply, _PAGE):
                self.kernel.stage_validated(reply.nodes)
            return reply

    def query(self, request):
        return self.query_summary(request, self.decode)

    def append(self):
        try:
            self.kernel.append()
        except self.native.AssemblyError as exc:
            raise ServerResponseError(str(exc)) from None
        except self.native.BudgetError as exc:
            raise QueryResponseError("resource_exhausted", str(exc)) from None

    def finish(self):
        try:
            return self.kernel.finish()
        except self.native.AssemblyError as exc:
            raise ServerResponseError(str(exc)) from None
