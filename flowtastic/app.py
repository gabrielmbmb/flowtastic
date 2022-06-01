from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, cast

from aiokafka import AIOKafkaConsumer
from flowtastic.logger import get_logger
from flowtastic.message import DeserializationError, Message
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from flowtastic.types import SubscriberFunc


logger = get_logger(__name__)


class _DeserializationCombo:
    """A combination of a `Message` class and a `BaseModel` class. This class is only used
    internally by the `FlowTastic` class to compare the combination of a `Message` class
    and a `BaseModel` class.

    Attributes:
        message: The `Message` class.
        pydantic_base_model: The `BaseModel` class.
    """

    def __init__(self, message: Message, pydantic_base_model: type[BaseModel]) -> None:
        """Inits `_DeserializationCombo` class.

        Args:
            message: The `Message` class.
            pydantic_base_model: The `BaseModel` class.
        """
        self.message = message
        self.pydantic_base_model = pydantic_base_model

    def __eq__(self, other: Any) -> bool:
        """Compares the `_DeserializationCombo` class with another `_DeserializationCombo`.
        They are equal if the `Message` class and the `BaseModel` class are the same (type).

        Args:
            other: The other `_DeserializationCombo` class.

        Returns:
            True if the `_DeserializationCombo` classes are equal, `False` otherwise.
        """
        if isinstance(other, _DeserializationCombo):
            return (
                self.message == other.message
                and self.pydantic_base_model == other.pydantic_base_model
            )
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash(
            (
                self.message.__class__.__name__,
                self.message.encoding,
                self.pydantic_base_model,
            )
        )


class FlowTastic:
    """FlowTastic is the entrypoint class for the library.

    Attributes:
        name: The name of the application.
        broker: The broker to connect to.
    """

    name: str
    broker: str

    _loop: asyncio.AbstractEventLoop
    _consume_task: asyncio.Task[None]
    _consumer: AIOKafkaConsumer
    _topic_to_model: dict[str, list[_DeserializationCombo]] = defaultdict(list)
    _model_to_subscriber: dict[
        _DeserializationCombo, list[SubscriberFunc]
    ] = defaultdict(list)

    def __init__(self, name: str, broker: str) -> None:
        """Init `FlowTastic` class.

        Args:
            name: The name of the FlowTastic application.
            broker: The host and port of the Kafka broker to connect to.
        """
        self.name = name
        self.broker = broker
        self._loop = asyncio.get_event_loop()

    def _register_deserialization_combo(
        self,
        topic: str,
        func: SubscriberFunc,
        message: Message,
        pydantic_base_model: type[BaseModel],
    ) -> None:
        """Register a `Message` class and a `BaseModel` class to the `_topic_to_model`
        dictionary.

        Args:
            topic: The topic to register the `Message` class and the `BaseModel` class to.
            func: The subscriber function to register.
            message: The `Message` class instance.
            pydantic_base_model: The `BaseModel` class type.
        """
        deserialization_combo = _DeserializationCombo(
            message=message,
            pydantic_base_model=pydantic_base_model,
        )
        if deserialization_combo not in self._topic_to_model[topic]:
            self._topic_to_model[topic].append(deserialization_combo)
        self._model_to_subscriber[deserialization_combo].append(func)

    def subscribe(self, topic: str) -> Callable[[SubscriberFunc], SubscriberFunc]:
        """A decorator to register a function as a subscriber of a topic.

        Args:
            topic: The topic to subscribe to.

        Raises:
            ValueError: If the function is not a coroutine (async function) or if the function
                has more than one parameter.
        """

        def register_subscriber(func: SubscriberFunc) -> SubscriberFunc:
            """Register a subscriber function for a topic. It validates that the function
            is a coroutine and that it has only one parameter. Then, if the type of the
            argument of the function is a `pydantic.BaseModel` it is associated with the
            `Message` class used (the default value of the argument) and the topic registering
            it to the `_topic_to_model` dictionary.

            Raises:
                ValueError: If the function has more than one argument.
            """
            if not asyncio.iscoroutinefunction(func):
                raise ValueError(
                    f"Subscriber functions must be coroutines (async). You're probably seeing "
                    f"this error because one of the function that you have decorated using "
                    f"`@{self.__class__.__name__}.subscribe` method is a regular function."
                )
            if func.__code__.co_argcount > 1:
                raise ValueError(
                    f"Subscriber functions must take only one argument. You're probably "
                    f"seeing this error because one of the function that you have decorated "
                    f"using `@{self.__class__.__name__}.subscribe` method takes more than "
                    f"one argument."
                )
            func_input_type = list(func.__annotations__.values())[0]
            func_input_message = cast(tuple[Any, ...], func.__defaults__)[0]
            if issubclass(func_input_type, BaseModel):
                self._register_deserialization_combo(
                    topic=topic,
                    func=func,
                    message=func_input_message,
                    pydantic_base_model=func_input_type,
                )
            else:
                pass
            return func

        return register_subscriber

    def publish(self, topic: str) -> None:
        """A decorator to register a function as a publisher of a topic.

        Args:
            topic: The topic to publish to.
        """
        pass

    def _consumed_topics(self) -> list[str]:
        """Returns the topics that should be consumed by the application.

        Returns:
            A list of topics to be consumed.
        """
        return list(self._topic_to_model.keys())

    def _create_consumer(self) -> None:
        """Creates an instance of `aiokafka.AIOKafkaConsumer` to consume messages from the
        Kafka Broker."""
        self._consumer = AIOKafkaConsumer(
            *self._consumed_topics(), bootstrap_servers=self.broker
        )

    async def _process_message(
        self,
        value: bytes,
        message: Message,
        pydantic_base_model: type[BaseModel] | None = None,
    ) -> BaseModel | Any | None:
        """Process the `value` attribute of a `aiokafka.ConsumerRecord` first deserializing
        the message and then converting it to a `pydantic.BaseModel` if needed. If the deserialization
        fails or the conversion to a `pydantic.BaseModel` fails, then no error is raised
        as it would cause the whole app to stop. Instead, a warning message is logged.

        Args:
            value: The `value` attribute of the `aiokafka.ConsumerRecord`.
            message: The message class to use for deserialization.
            pydantic_base_model: The `pydantic.BaseModel` class to use for deserialization.

        Returns:
            If the deserialization and conversion to a `pydantic.BaseModel` succeed, then
            the deserialized message is returned. Otherwise, `None` is returned.
        """
        try:
            deserialized = message.deserialize(value)
            if pydantic_base_model:
                return pydantic_base_model(**deserialized)
            return deserialized
        except DeserializationError as e:
            logger.warning(f"Failed to deserialize message: {e}")
        except ValidationError as e:
            logger.warning(
                f"Could not create '{pydantic_base_model.__class__.__name__}' from deserialized message: {e}"
            )
        return None

    async def _distribute_message(self, record: ConsumerRecord) -> None:
        """Distributes the message to the subscribers. For that, it first deserializes the
        message and then pass it to the subscribers.

        Args:
            record: The `aiokafka.ConsumerRecord` to distribute.
        """
        topic = record.topic
        for deserialization_combo in self._topic_to_model[topic]:
            python_object = await self._process_message(
                record.value,
                deserialization_combo.message,
                deserialization_combo.pydantic_base_model,
            )
            if python_object:
                for subscriber in self._model_to_subscriber[deserialization_combo]:
                    await subscriber(python_object)

    async def _consume(self) -> None:
        """Start consuming messages from the Kafka broker."""
        self._create_consumer()
        await self._consumer.start()
        try:
            async for msg in self._consumer:
                await self._distribute_message(msg)
        finally:
            await self._consumer.stop()

    def run(self) -> None:
        """Run the application."""
        self._consume_task = self._loop.create_task(self._consume())
        try:
            self._loop.run_until_complete(self._consume_task)
        except asyncio.CancelledError:
            pass
        finally:
            self._loop.run_until_complete(self._consumer.stop())
            self._loop.close()
