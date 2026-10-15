#!/usr/bin/env python3
import time

import requests


NODES = (
    {"name": "pg1", "port": 55441, "api": "http://127.0.0.1:8008"},
    {"name": "pg2", "port": 55442, "api": "http://127.0.0.1:8009"},
)


def cluster_state():
    state = []
    for node in NODES:
        response = requests.get(f"{node['api']}/patroni", timeout=3)
        response.raise_for_status()
        details = response.json()
        state.append({**node, **details})
    return state


def leader_and_replica():
    state = cluster_state()
    leaders = [node for node in state if node["role"] in {"leader", "primary", "master"}]
    replicas = [node for node in state if node["role"] == "replica"]
    if len(leaders) != 1 or len(replicas) != 1:
        roles = [(node["name"], node.get("role"), node.get("state")) for node in state]
        raise RuntimeError(f"expected one leader and one replica, got {roles}")
    return leaders[0], replicas[0]


def dsn(node, database="hw3"):
    return (
        f"host=127.0.0.1 port={node['port']} dbname={database} "
        "user=postgres password=postgres"
    )


def wait_for_roles(timeout=90):
    deadline = time.monotonic() + timeout
    last_error = None
    while time.monotonic() < deadline:
        try:
            return leader_and_replica()
        except (requests.RequestException, RuntimeError) as error:
            last_error = error
            time.sleep(2)
    raise TimeoutError(f"Patroni roles did not stabilize: {last_error}")
