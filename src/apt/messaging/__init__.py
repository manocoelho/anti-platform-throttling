"""Mensageria: topologia RabbitMQ, publisher e consumer.

A topologia (exchanges topic + fanout, DLX/DLQ e filas de retry com TTL) esta
documentada em detalhe no docstring de `topology.py` -- incluindo o porque das
filas de retry em vez de `sleep()` no worker.
"""

from apt.messaging.consumer import Consumer, read_header_int
from apt.messaging.publisher import Publisher, close_publisher, get_publisher
from apt.messaging.topology import (
    EXCHANGE_CONTROL,
    EXCHANGE_DLX,
    EXCHANGE_RETRY,
    EXCHANGE_TASKS,
    QUEUE_DLQ,
    RETRY_TIERS_MS,
    Topology,
    declare_control_queue,
    declare_topology,
    retry_queue_name,
    retry_routing_key,
    task_queue_name,
)

__all__ = [
    "EXCHANGE_CONTROL",
    "EXCHANGE_DLX",
    "EXCHANGE_RETRY",
    "EXCHANGE_TASKS",
    "QUEUE_DLQ",
    "RETRY_TIERS_MS",
    "Consumer",
    "Publisher",
    "Topology",
    "close_publisher",
    "declare_control_queue",
    "declare_topology",
    "get_publisher",
    "read_header_int",
    "retry_queue_name",
    "retry_routing_key",
    "task_queue_name",
]
