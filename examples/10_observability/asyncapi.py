"""Generate an AsyncAPI schema for the Celery topology.

Every subscriber and publisher contributes a channel with its queue, exchange
and routing key, so the schema documents what this service consumes and
produces without a broker running.

The FastStream CLI does the same thing:
    faststream docs gen faststream_app:app        # needs faststream[cli]

Run:
    python asyncapi.py > asyncapi.json
"""

import json
import sys

from faststream.specification import AsyncAPI

from faststream_app import broker


def main() -> int:
    specification = AsyncAPI(
        broker,
        title="Payments tasks",
        version="1.0.0",
        description="Celery tasks this service consumes and publishes.",
    ).to_specification()

    json.dump(specification.to_jsonable(), sys.stdout, indent=2)
    print()

    return 0


if __name__ == "__main__":
    sys.exit(main())
