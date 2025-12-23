from dagster_openlineage.adapter import OpenLineageAdapter


class OpenLineageEventListener:
    def __init__(self, adapter: OpenLineageAdapter):
        self.adapter = adapter
