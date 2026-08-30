import json

import pytest

from faststream_celery.parser import CeleryParser
from tests.helpers import consumer_message, task_message, v1_task_message


@pytest.fixture()
def parser() -> CeleryParser:
    return CeleryParser()


class TestProtocolV2:
    @pytest.mark.asyncio()
    async def test_headers_and_body_are_normalized(self, parser: CeleryParser) -> None:
        msg = await parser.parse_message(
            task_message(
                args=[1, 2],
                kwargs={"debug": True},
                properties={
                    "correlation_id": "task-id-1",
                    "reply_to": "reply-queue",
                },
            ),
        )

        assert msg.headers["task"] == "proj.tasks.add"
        assert msg.message_id == "task-id-1"
        assert msg.correlation_id == "task-id-1"
        assert msg.reply_to == "reply-queue"
        assert json.loads(msg.body) == {"args": [1, 2], "kwargs": {"debug": True}}

    @pytest.mark.asyncio()
    async def test_missing_properties_fall_back(self, parser: CeleryParser) -> None:
        msg = await parser.parse_message(task_message(task_id="task-id-2"))

        assert msg.reply_to == ""
        assert msg.message_id == "task-id-2"
        assert msg.correlation_id

    @pytest.mark.asyncio()
    async def test_body_is_decoded_to_args_and_kwargs(
        self,
        parser: CeleryParser,
    ) -> None:
        msg = await parser.parse_message(task_message(args=["a"], kwargs={"b": 1}))

        assert await parser.decode_message(msg) == {"args": ["a"], "kwargs": {"b": 1}}

    @pytest.mark.asyncio()
    async def test_extra_embed_slot_is_tolerated(self, parser: CeleryParser) -> None:
        """A body with more than three slots still yields args and kwargs."""
        msg = await parser.parse_message(
            consumer_message(
                [[1], {"a": 2}, {}, {"extra": "slot"}],
                headers={"task": "proj.tasks.add"},
            ),
        )

        assert json.loads(msg.body) == {"args": [1], "kwargs": {"a": 2}}

    @pytest.mark.asyncio()
    async def test_a_body_that_is_not_a_sequence_is_rejected(
        self,
        parser: CeleryParser,
    ) -> None:
        with pytest.raises(ValueError, match="Invalid Celery protocol v2 body"):
            await parser.parse_message(
                consumer_message(
                    {"not": "a sequence"},
                    headers={"task": "proj.tasks.add"},
                ),
            )

    @pytest.mark.asyncio()
    async def test_a_short_body_is_rejected(self, parser: CeleryParser) -> None:
        with pytest.raises(ValueError, match="Invalid Celery protocol v2 body"):
            await parser.parse_message(
                consumer_message([[1, 2]], headers={"task": "proj.tasks.add"}),
            )

    @pytest.mark.asyncio()
    async def test_an_empty_task_header_is_not_protocol_v2(
        self,
        parser: CeleryParser,
    ) -> None:
        """An empty `task` header means the header is absent, not a v2 body."""
        msg = await parser.parse_message(
            consumer_message({"payload": 1}, headers={"task": ""}),
        )

        assert await parser.decode_message(msg) == {"payload": 1}


class TestProtocolV1:
    @pytest.mark.asyncio()
    async def test_flat_body_becomes_v2_shaped_headers(
        self,
        parser: CeleryParser,
    ) -> None:
        msg = await parser.parse_message(
            v1_task_message(
                args=[1, 2],
                kwargs={"debug": True},
                retries=3,
                eta="2026-01-01T00:00:00+00:00",
                taskset="group-1",
            ),
        )

        assert msg.headers["task"] == "proj.tasks.add"
        assert msg.headers["id"] == "task-id-1"
        assert msg.headers["retries"] == 3
        assert msg.headers["eta"] == "2026-01-01T00:00:00+00:00"
        assert msg.headers["group"] == "group-1"
        assert msg.message_id == "task-id-1"
        assert json.loads(msg.body) == {"args": [1, 2], "kwargs": {"debug": True}}

    @pytest.mark.asyncio()
    async def test_optional_fields_get_defaults(self, parser: CeleryParser) -> None:
        msg = await parser.parse_message(
            consumer_message({"task": "proj.tasks.add", "id": "task-id-2"}),
        )

        assert msg.headers["retries"] == 0
        assert msg.headers["timelimit"] == [None, None]
        assert msg.headers["group"] is None
        assert json.loads(msg.body) == {"args": [], "kwargs": {}}

    @pytest.mark.asyncio()
    async def test_null_args_become_empty(self, parser: CeleryParser) -> None:
        msg = await parser.parse_message(
            consumer_message(
                {"task": "proj.tasks.add", "args": None, "kwargs": None},
            ),
        )

        assert json.loads(msg.body) == {"args": [], "kwargs": {}}


class TestPlainMessages:
    @pytest.mark.asyncio()
    async def test_a_result_envelope_is_passed_through(
        self,
        parser: CeleryParser,
    ) -> None:
        """A Celery reply has no `task` anywhere; it stays a plain payload."""
        result = {
            "task_id": "task-id-1",
            "status": "SUCCESS",
            "result": 3,
            "traceback": None,
            "children": [],
        }

        msg = await parser.parse_message(
            consumer_message(result, properties={"correlation_id": "task-id-1"}),
        )

        assert msg.correlation_id == "task-id-1"
        assert await parser.decode_message(msg) == result

    @pytest.mark.asyncio()
    async def test_a_list_payload_is_not_mistaken_for_a_task(
        self,
        parser: CeleryParser,
    ) -> None:
        msg = await parser.parse_message(consumer_message([1, 2, 3]))

        assert await parser.decode_message(msg) == [1, 2, 3]

    @pytest.mark.asyncio()
    async def test_an_undecodable_body_reaches_the_decoder_untouched(
        self,
        parser: CeleryParser,
    ) -> None:
        """An unknown content type is not a parsing error at this stage."""
        msg = await parser.parse_message(
            consumer_message(
                {"payload": 1},
                content_type="application/x-unknown",
            ),
        )

        assert msg.body == b'{"payload": 1}'
        assert msg.headers == {}
