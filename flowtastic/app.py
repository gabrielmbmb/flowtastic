from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable, cast

from aiokafka import AIOKafkaConsumer
from flowtastic._deserialization_combo import _DeserializationCombo
from flowtastic.logger import get_logger
from flowtastic.message import DeserializationError, Message
from flowtastic.types import DeserializationErrorFunc, ValidationErrorFunc
from flowtastic.utils.asynchronous import create_gathering
from flowtastic.utils.pydantic_models import is_base_model_subclass
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from flowtastic.types import SubscriberFunc


logger = get_logger(__name__)


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
    _topic_to_deserializer: dict[
        str, list[Message | _DeserializationCombo]
    ] = defaultdict(list)
    _deserializer_to_subscriber: dict[
        _DeserializationCombo | Message, list[SubscriberFunc]
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

    def _register_deserializer(
        self,
        topic: str,
        func: SubscriberFunc,
        deserializer: Message | _DeserializationCombo,
    ) -> None:
        """Register a `Message` class or a `_DeserializationCombo` to the `_topic_to_deserializer`
        dictionary and the subscriber function to the `_deserializer_to_subscriber` dictionary.

        Args:
            topic: The topic to which `deserializer` should be registered.
            func: The subscriber function to register.
            deserializer: The `Message` class or the `_DeserializationCombo` class.
        """
        if deserializer not in self._topic_to_deserializer[topic]:
            self._topic_to_deserializer[topic].append(deserializer)
        self._deserializer_to_subscriber[deserializer].append(func)

    def subscriber(
        self,
        topic: str,
        on_deserialization_error: DeserializationErrorFunc | None = None,
        on_validation_error: ValidationErrorFunc | None = None,
    ) -> Callable[[SubscriberFunc], SubscriberFunc]:
        """A decorator to register a function as a subscriber of a topic.

        Args:
            topic: The topic to subscribe to.
            on_deserialization_error: The function to call when a `flowtastic.message.DeserializationError`
                occurs. If not provided, then the error will be ignored.
            on_validation_error: The function to call when a `pydantic.ValidationError`
                occurs. If not provided, then the error will be ignored.

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
            # TODO: handle no annotations
            func_input_type = list(func.__annotations__.values())[0]
            # TODO: handle default message if not provided
            func_input_message = cast(tuple[Any, ...], func.__defaults__)[0]
            if is_base_model_subclass(func_input_type):
                logger.debug(
                    f"Registering {func} as subscriber for '{topic}' topic with deserialization "
                    f"of `{func_input_message}` to `{func_input_type}` `pydantic.BaseModel`"
                )
                deserializer = _DeserializationCombo(
                    message=func_input_message, pydantic_base_model=func_input_type
                )
            else:
                logger.debug(
                    f"Registering {func} as subscriber for '{topic}' topic with deserialization "
                    f"of `{func_input_message}`"
                )
                deserializer = func_input_message
            self._register_deserializer(
                topic=topic, func=func, deserializer=deserializer
            )
            return func

        return register_subscriber

    def _consumed_topics(self) -> list[str]:
        """Returns the topics that should be consumed by the application.

        Returns:
            A list of topics to be consumed.
        """
        return list(self._topic_to_deserializer.keys())

    def _create_consumer(self) -> None:
        """Creates an instance of `aiokafka.AIOKafkaConsumer` to consume messages from the
        Kafka Broker."""
        self._consumer = AIOKafkaConsumer(
            *self._consumed_topics(), bootstrap_servers=self.broker
        )

    async def _process_message(
        self, value: bytes, deserializer: Message | _DeserializationCombo
    ) -> BaseModel | Any | None:
        """Process the `value` attribute of a `aiokafka.ConsumerRecord` deserializing the
        message and if needed, converting it to a `pydantic.BaseModel`. If the deserialization
        fails or the conversion to a `pydantic.BaseModel` fails, then the provided callbacks
        functions for each error are called.

        Args:
            value: The `value` attribute of the `aiokafka.ConsumerRecord`.
            deserializer: A `Message` class to deserialize the `aiokafka.ConsumerRecord.value`
                to a Python object or a `_DeserializationCombo` object to deserialize the
                `aiokafka.ConsumerRecord.value` to a `pydantic.BaseModel`.

        Returns:
            If the deserialization and conversion to a `pydantic.BaseModel` succeed, then
            the deserialized message is returned. Otherwise, `None` is returned.
        """
        try:
            # Just deserialize the message using the provided deserializer
            if isinstance(deserializer, Message):
                logger.debug(f"Deserializing message using `{deserializer}`...")
                return deserializer.deserialize(value)
            # We have a deserialization combo, so first deserialize the message and then
            # convert it to the provided `pydantic.BaseModel` subclass.
            logger.debug(
                f"Deserializing message using `{deserializer.message}` and then converting "
                f"it to `{deserializer.pydantic_base_model}`..."
            )
            message = deserializer.message.deserialize(value)
            return deserializer.pydantic_base_model(**message)
        except DeserializationError as e:  # noqa
            # TODO: handle deserialization error using `Message.on_deserialization_error`
            pass
        except ValidationError as e:  # noqa
            # TODO: handle deserialization error using `BaseModel.on_validation_error`
            pass
        return None

    async def _subscriber_wrapper(
        self, func: SubscriberFunc, arg: BaseModel | Any
    ) -> None:
        """Wraps the subscriber function to handle the return value of the function if needed.

        Args:
            func: The subscriber function to wrap.
            arg: The argument to pass to the subscriber function.
        """
        result = await func(arg)  # noqa
        # TODO: handle result if the subscriber function has `Publish` annotation.

    async def _distribute_message(self, record: ConsumerRecord) -> None:
        """Distributes the message to the subscribers. For that, it first deserializes the
        message and then pass it to the subscribers.

        Args:
            record: The `aiokafka.ConsumerRecord` to distribute.
        """
        topic = record.topic
        to_be_awaited = []
        for deserializer in self._topic_to_deserializer[topic]:
            python_object = await self._process_message(
                record.value, deserializer=deserializer
            )
            if not python_object:
                continue
            to_be_awaited.append(
                create_gathering(
                    self._deserializer_to_subscriber[deserializer],
                    self._subscriber_wrapper,
                    python_object,
                )
            )
        asyncio.gather(*to_be_awaited)

    async def _consume(self) -> None:
        """Start consuming messages from the Kafka broker."""
        self._create_consumer()
        await self._consumer.start()
        try:
            async for msg in self._consumer:
                # TODO: create a thread for each topic consumed
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
        except KeyboardInterrupt:
            pass
        finally:
            self._loop.run_until_complete(self._consumer.stop())
            self._loop.close()
