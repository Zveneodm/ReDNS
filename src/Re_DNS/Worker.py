"""Per-client workload source; experiment orchestration lives in sim/."""


class Worker:
    """One workload source per client; a client keeps its forwarder throughout a run."""

    def __init__(self, client):
        self.client = client

    def send_query(self, query):
        return self.client.send_query(query)
