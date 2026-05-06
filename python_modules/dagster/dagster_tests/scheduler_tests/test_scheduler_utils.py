import datetime
from unittest.mock import MagicMock

from dagster._core.definitions.run_request import RunRequest
from dagster._core.storage.dagster_run import RunsFilter
from dagster._core.storage.tags import (
    GUARANTEED_GLOBALLY_UNIQUE_RUN_KEY_TAG,
    RUN_KEY_TAG,
    SCHEDULE_NAME_TAG,
    SCHEDULED_EXECUTION_TIME_TAG,
)
from dagster._scheduler.scheduler import (
    _get_next_scheduler_iteration_time,
    _scheduled_execution_time_iso,
    _tags_for_scheduled_execution_time,
)

MINUTE_BOUNDARY = 1670596320


def test_next_iteration_time():
    assert MINUTE_BOUNDARY % 60 == 0

    assert _get_next_scheduler_iteration_time(MINUTE_BOUNDARY) == MINUTE_BOUNDARY + 60
    assert _get_next_scheduler_iteration_time(MINUTE_BOUNDARY + 0.01) == MINUTE_BOUNDARY + 60
    assert _get_next_scheduler_iteration_time(MINUTE_BOUNDARY + 30) == MINUTE_BOUNDARY + 60
    assert _get_next_scheduler_iteration_time(MINUTE_BOUNDARY + 59.99) == MINUTE_BOUNDARY + 60

    assert _get_next_scheduler_iteration_time(MINUTE_BOUNDARY + 60) == MINUTE_BOUNDARY + 120


def test_scheduled_execution_time_iso_converts_to_utc():
    # UTC datetime is returned unchanged.
    dt_utc = datetime.datetime(2024, 1, 15, 12, 0, 0, tzinfo=datetime.timezone.utc)
    assert _scheduled_execution_time_iso(dt_utc) == "2024-01-15T12:00:00+00:00"

    # Non-UTC timezone is normalized to UTC.
    tz_plus5 = datetime.timezone(datetime.timedelta(hours=5))
    dt_plus5 = datetime.datetime(2024, 1, 15, 17, 0, 0, tzinfo=tz_plus5)
    assert _scheduled_execution_time_iso(dt_plus5) == "2024-01-15T12:00:00+00:00"


def test_scheduled_execution_time_iso_naive_datetime_behavior():
    # Naive datetimes are an unsupported/incidental input; this test documents observed
    # behavior but does not assert it as a contract. In practice, schedule_time is always
    # timezone-aware (UTC). Naive datetimes are treated as local time by astimezone(), which
    # is OS-locale-dependent. We can't assert a specific UTC value, but we can assert the
    # result is a valid UTC ISO string and that two naive datetimes that differ by a known
    # offset produce results that differ by that same offset.
    dt_naive_a = datetime.datetime(2024, 1, 15, 12, 0, 0)
    dt_naive_b = datetime.datetime(2024, 1, 15, 13, 0, 0)

    result_a = _scheduled_execution_time_iso(dt_naive_a)
    result_b = _scheduled_execution_time_iso(dt_naive_b)

    # Both results should be parseable as UTC ISO strings.
    parsed_a = datetime.datetime.fromisoformat(result_a)
    parsed_b = datetime.datetime.fromisoformat(result_b)
    assert parsed_a.tzinfo == datetime.timezone.utc
    assert parsed_b.tzinfo == datetime.timezone.utc

    # The UTC values should differ by exactly 1 hour.
    assert parsed_b - parsed_a == datetime.timedelta(hours=1)


def _make_remote_schedule(name: str) -> MagicMock:
    mock = MagicMock()
    mock.name = name
    return mock


def _make_run_request(run_key: str | None) -> RunRequest:
    return RunRequest(run_key=run_key)


def test_tags_for_scheduled_execution_time_no_run_key():
    schedule_time = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    remote_schedule = _make_remote_schedule("my_schedule")
    run_request = _make_run_request(None)

    tags, unique_key, runs_filter = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, run_request
    )

    expected_iso = "2024-06-01T00:00:00+00:00"
    assert tags[SCHEDULE_NAME_TAG] == "my_schedule"
    assert tags[SCHEDULED_EXECUTION_TIME_TAG] == expected_iso
    assert RUN_KEY_TAG not in tags
    assert unique_key == f"schedule:name=my_schedule,run_key=,time={expected_iso}"
    assert runs_filter == RunsFilter(tags={SCHEDULED_EXECUTION_TIME_TAG: expected_iso})


def test_tags_for_scheduled_execution_time_with_run_key():
    schedule_time = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    remote_schedule = _make_remote_schedule("my_schedule")
    run_request = _make_run_request("partition_A")

    tags, unique_key, runs_filter = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, run_request
    )

    expected_iso = "2024-06-01T00:00:00+00:00"
    assert tags[SCHEDULE_NAME_TAG] == "my_schedule"
    assert tags[SCHEDULED_EXECUTION_TIME_TAG] == expected_iso
    assert tags[RUN_KEY_TAG] == "partition_A"
    assert unique_key == f"schedule:name=my_schedule,run_key=partition_A,time={expected_iso}"
    assert runs_filter == RunsFilter(tags={SCHEDULED_EXECUTION_TIME_TAG: expected_iso})


def test_tags_for_scheduled_execution_time_unique_key_differs_by_run_key():
    schedule_time = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    remote_schedule = _make_remote_schedule("my_schedule")

    _, key_no_run_key, _ = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, _make_run_request(None)
    )
    _, key_run_key_a, _ = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, _make_run_request("partition_A")
    )
    _, key_run_key_b, _ = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, _make_run_request("partition_B")
    )
    assert key_no_run_key != key_run_key_a
    assert key_run_key_a != key_run_key_b


def test_tags_for_scheduled_execution_time_unique_key_differs_by_schedule_name():
    schedule_time = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    run_request = _make_run_request("same_key")

    _, key_a, _ = _tags_for_scheduled_execution_time(
        _make_remote_schedule("schedule_a"), schedule_time, run_request
    )
    _, key_b, _ = _tags_for_scheduled_execution_time(
        _make_remote_schedule("schedule_b"), schedule_time, run_request
    )
    assert key_a != key_b


def test_tags_for_scheduled_execution_time_guaranteed_unique_key_not_written_to_tags():
    # The GUARANTEED_GLOBALLY_UNIQUE_RUN_KEY_TAG is intentionally NOT included in the
    # returned tags dict — it is written separately in _create_scheduler_run so that
    # _get_existing_run_for_request can compare tags without the new hidden tag
    # (which won't exist on older runs).
    schedule_time = datetime.datetime(2024, 6, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    remote_schedule = _make_remote_schedule("my_schedule")

    tags, _, _ = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, _make_run_request(None)
    )
    assert GUARANTEED_GLOBALLY_UNIQUE_RUN_KEY_TAG not in tags

    tags_with_key, _, _ = _tags_for_scheduled_execution_time(
        remote_schedule, schedule_time, _make_run_request("k")
    )
    assert GUARANTEED_GLOBALLY_UNIQUE_RUN_KEY_TAG not in tags_with_key
