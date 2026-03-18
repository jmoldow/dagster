import time
from typing import Any

from dagster._core.remote_representation.grpc_server_state_subscriber import (
    LocationStateChangeEventType,
    LocationStateSubscriber,
)

from dagster_graphql_tests.graphql.graphql_context_test_suite import (
    GraphQLContextVariant,
    make_graphql_context_test_suite,
)

BaseTestSuite: Any = make_graphql_context_test_suite(
    context_variants=[GraphQLContextVariant.non_launchable_sqlite_instance_deployed_grpc_env()]
)


class TestSubscribeToGrpcServerEvents(BaseTestSuite):
    def test_grpc_server_handle_message_subscription(self, graphql_context):
        events = []
        test_subscriber = LocationStateSubscriber(events.append)
        location = next(
            iter(graphql_context.process_context.create_request_context().code_locations)
        )
        graphql_context.process_context.add_state_subscriber(test_subscriber)
        location.client.shutdown_server()

        # Wait for LOCATION_ERROR event. LOCATION_DISCONNECTED may arrive first since the
        # watch thread detects the disconnect before exhausting reconnect attempts.
        start_time = time.time()
        timeout = 60
        while not any(e.event_type == LocationStateChangeEventType.LOCATION_ERROR for e in events):
            if time.time() - start_time > timeout:
                raise Exception("Timed out waiting for LOCATION_ERROR event")
            time.sleep(1)

        error_events = [
            e for e in events if e.event_type == LocationStateChangeEventType.LOCATION_ERROR
        ]
        assert len(error_events) == 1
        assert error_events[0].location_name == location.name

        # LOCATION_DISCONNECTED should have arrived before LOCATION_ERROR
        disconnect_events = [
            e for e in events if e.event_type == LocationStateChangeEventType.LOCATION_DISCONNECTED
        ]
        assert len(disconnect_events) == 1
        assert disconnect_events[0].location_name == location.name
