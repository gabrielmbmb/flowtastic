from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, DefaultDict, Dict, List, Union

from aiokafka import AIOKafkaProducer
from flowtastic.message.json_message import JSONMessage

if TYPE_CHECKING:
    from flowtastic.message import Message


class Publish:
    """A class used to indicate that the return value of a subscriber function should be
    published to the specified topics."""

    _message_to_topics: DefaultDict[Message, List[str]] = defaultdict(list)

    def __init__(
        self,
        to_topics: Dict[str, Union[Message, None]] | List[str],
        default_message: Message = JSONMessage(),
    ) -> None:
        """Inits `Publish` class.

        Args:
            to_topics: it can be a `dict` in which the keys are the name of the topics where
                the message should be published to and the values are the `Message` class
                instance used to serialize it, or a `list` containing the name of the topics
                where the message should be published.
            default_message: an instance of `Message` class that will be used to serialize
                the Python object when `to_topics` is a `list`, then it will be used to serialize
                the Python object before publishing it to the topics. If `to_topics` is
                a `dict`, and some key has `None` as value, then it will be used to serialize
                the Python object that will be published to the topic.
        """
        if isinstance(to_topics, dict):
            for topic, message in to_topics.items():
                if message:
                    self._message_to_topics[message].append(topic)
                else:
                    self._message_to_topics[default_message].append(topic)
        else:
            self._message_to_topics[default_message] = to_topics

    async def publish(self, producer: AIOKafkaProducer, python_object: Any) -> None:
        """Serializes `python_object` using the `Message` class in `_message_to_topics`
        and then publish the serialized message to the topics.

        Args:
            producer: the Kafka Producer used to publish the serialized message.
            python_object: the Python object to be serialized and published to topics.
        """
        for message, topics in self._message_to_topics.items():
            serialized = message.serialize(python_object)
            await asyncio.gather(
                *[
                    producer.send_and_wait(topic=topic, value=serialized)
                    for topic in topics
                ]
            )
