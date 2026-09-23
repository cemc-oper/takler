"""Response association checks shared by the two Python wire transports."""

from takler.exceptions import ServerResponseError
from takler.protocol.commands import BatchResponse, Command


def validate_batch_response(command, payload, response):
    if not isinstance(response, BatchResponse):
        raise ServerResponseError("expected batch response")
    if not response.results and response.flag not in (0, 16):
        return response  # Request-level rejection.
    expected = (
        (None if not payload.get("flow_name") else ["/" + payload["flow_name"]])
        if command == Command.BEGIN
        else payload.get("node_paths", payload.get("paths"))
    )
    if expected is not None and [r.target for r in response.results] != list(expected):
        raise ServerResponseError("batch targets do not match request")
    if [r.index for r in response.results] != list(range(len(response.results))):
        raise ServerResponseError("invalid batch result indices")
    if response.flag != (16 if any(r.flag != 0 for r in response.results) else 0):
        raise ServerResponseError("invalid batch aggregate flag")
    return response
