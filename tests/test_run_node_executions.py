from typing import Any, Dict, List, Optional

import pytest

from paradime.apis.bolt.client import BoltClient


def _node(unique_id: str, **overrides: Any) -> Dict[str, Any]:
    node: Dict[str, Any] = {
        "commandId": 7,
        "invocationId": "inv-1",
        "dbtVersion": "1.10.0",
        "uniqueId": unique_id,
        "resourceType": "model",
        "status": "success",
        "message": "OK",
        "failures": None,
        "threadId": "Thread-1",
        "compileStartedAt": "2026-10-22T10:00:00+00:00",
        "compileCompletedAt": "2026-10-22T10:00:01+00:00",
        "executeStartedAt": "2026-10-22T10:00:01+00:00",
        "executeCompletedAt": "2026-10-22T10:00:03+00:00",
        "executionTime": 2.5,
        "adapterResponse": '{"rows_affected": 3}',
    }
    node.update(overrides)
    return node


def _page(
    nodes: List[Dict[str, Any]], has_next_page: bool, end_cursor: Optional[str]
) -> Dict[str, Any]:
    return {
        "boltRunNodeExecutions": {
            "edges": [{"node": n, "cursor": f"c-{n['uniqueId']}"} for n in nodes],
            "pageInfo": {"hasNextPage": has_next_page, "endCursor": end_cursor},
        }
    }


class FakeAPIClient:
    """Returns queued responses and records the variables of each call."""

    def __init__(self, responses: List[Dict[str, Any]]) -> None:
        self.responses = responses
        self.calls: List[Dict[str, Any]] = []
        self.query: Optional[str] = None

    def _call_gql(self, *, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        self.query = query
        self.calls.append(variables)
        return self.responses.pop(0)


def _bolt(responses: List[Dict[str, Any]]) -> BoltClient:
    return BoltClient(FakeAPIClient(responses))  # type: ignore[arg-type]


def test_page_parses_node_and_adapter_response() -> None:
    bolt = _bolt([_page([_node("model.proj.a")], has_next_page=False, end_cursor="c1")])

    page = bolt.get_run_node_executions_page(123)

    assert bolt.client.calls == [{"runId": 123, "first": 100, "after": None}]  # type: ignore[attr-defined]
    assert "boltRunNodeExecutions(runId: $runId" in bolt.client.query  # type: ignore[attr-defined, operator]
    assert page.has_next_page is False
    assert page.end_cursor == "c1"
    (node,) = page.node_executions
    assert node.unique_id == "model.proj.a"
    assert node.command_id == 7
    assert node.resource_type == "model"
    assert node.execution_time == 2.5
    assert node.adapter_response == {"rows_affected": 3}


def test_null_fields_and_missing_adapter_response() -> None:
    bolt = _bolt(
        [
            _page(
                [_node("test.proj.t", status="fail", failures=2, adapterResponse=None)],
                has_next_page=False,
                end_cursor=None,
            )
        ]
    )

    (node,) = bolt.get_run_node_executions_page(1).node_executions

    assert node.status == "fail"
    assert node.failures == 2
    assert node.adapter_response is None


def test_list_follows_cursor_across_two_pages() -> None:
    bolt = _bolt(
        [
            _page([_node("model.proj.a"), _node("model.proj.b")], True, "cursor-1"),
            _page([_node("model.proj.c")], False, "cursor-2"),
        ]
    )

    nodes = bolt.list_run_node_executions(55)

    assert [n.unique_id for n in nodes] == ["model.proj.a", "model.proj.b", "model.proj.c"]
    assert bolt.client.calls == [  # type: ignore[attr-defined]
        {"runId": 55, "first": 500, "after": None},
        {"runId": 55, "first": 500, "after": "cursor-1"},
    ]


def test_list_returns_empty_when_run_has_no_node_rows() -> None:
    bolt = _bolt([_page([], has_next_page=False, end_cursor=None)])

    assert bolt.list_run_node_executions(9) == []


def test_null_connection_returns_empty_page() -> None:
    bolt = _bolt([{"boltRunNodeExecutions": None}])

    page = bolt.get_run_node_executions_page(9)

    assert page.node_executions == []
    assert page.has_next_page is False


@pytest.mark.parametrize("first", [0, 501])
def test_page_size_out_of_range_rejected(first: int) -> None:
    with pytest.raises(ValueError):
        _bolt([]).get_run_node_executions_page(1, first=first)
