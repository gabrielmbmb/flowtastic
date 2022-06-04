from __future__ import annotations

import asyncio
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Callable

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from flowtastic._deserialization_combo import _DeserializationCombo
from flowtastic.logger import get_logger
from flowtastic.message import DeserializationError, JSONMessage, Message
from flowtastic.publish import Publish
from flowtastic.types import DeserializationErrorFunc, ValidationErrorFunc
from flowtastic.utils.asynchronous import create_gathering
from flowtastic.utils.list import common
from flowtastic.utils.pydantic_models import is_base_model_subclass
from pydantic import BaseModel, ValidationError

if TYPE_CHECKING:
    from aiokafka import ConsumerRecord
    from flowtastic.types import SubscriberFunc


logger = get_logger(__name__)


def _get_subscriber_func_info(
    func: SubscriberFunc,
) -> tuple[type[BaseModel] | Any | None, Message | None, Publish | None]:
    """Returns the type and default value of the argument, and the return type of one
    `SubscriberFunc`.

    Args:
        func: The `SubscriberFunc` to get the type and default value of the argument, and the
            return type of.

    Returns:
        A `tuple` containing the type and default value of the argument, and the return type of
        the `SubscriberFunc`.
    """
    annotations = func.__annotations__
    annotations_num = len(annotations)
    defaults = func.__defaults__
    if annotations_num >= 2 or (annotations_num == 1 and "return" not in annotations):
        func_input_type = list(annotations.values())[0]
    else:
        func_input_type = None
    func_output_type = annotations["return"] if "return" in annotations else None
    func_input_default = defaults[0] if defaults else None
    return func_input_type, func_input_default, func_output_type


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
    _producer: AIOKafkaProducer
    _topic_to_subscriber: dict[str, list[SubscriberFunc]] = defaultdict(list)
    _topic_to_deserializer: dict[
        str, list[Message | _DeserializationCombo]
    ] = defaultdict(list)
    _deserializer_to_subscriber: dict[
        _DeserializationCombo | Message, list[SubscriberFunc]
    ] = defaultdict(list)
    _subscriber_to_publish: dict[SubscriberFunc, Publish] = {}

    def __init__(self, name: str, broker: str) -> None:
        """Init `FlowTastic` class.

        Args:
            name: The name of the FlowTastic application.
            broker: The host and port of the Kafka broker to connect to.
        """
        self.name = name
        self.broker = broker
        self._loop = asyncio.get_event_loop()

    def _register_subscriber(
        self,
        topic: str,
        func: SubscriberFunc,
        deserializer: Message | _DeserializationCombo,
        publish: Publish | None = None,
    ) -> None:
        """Register a `Message` class or a `_DeserializationCombo` to the `_topic_to_deserializer`
        dictionary, the subscriber function to the `_deserializer_to_subscriber` dictionary
        and the `Publish` to the `_subscriber_to_publish` dictionary.

        Args:
            topic: The topic to which `deserializer` should be registered.
            func: The subscriber function to register.
            deserializer: The `Message` class or the `_DeserializationCombo` class.
            publish: The `Publish` class to register.
        """
        self._topic_to_subscriber[topic].append(func)
        if deserializer not in self._topic_to_deserializer[topic]:
            self._topic_to_deserializer[topic].append(deserializer)
        self._deserializer_to_subscriber[deserializer].append(func)
        if publish:
            self._subscriber_to_publish[func] = publish

    def subscriber(
        self,
        topic: str,
        on_deserialization_error: DeserializationErrorFunc | None = None,
        on_validation_error: ValidationErrorFunc | None = None,
    ) -> Callable[[SubscriberFunc], SubscriberFunc]:
        """A decorator to register a function as a subscriber of a topic. A subscriber function
        should accept a single argument and the type of this argument has to be a `BaseModel`
        or type hints from `typing` module. The default value of this argument should be
        a subclass of `Message` that will be used to deserialize the message. If no default
        value is provided, then the `JSONMessage` class will be used. Additionaly, a subscriber
        func return type can be annotated using an instance of the `Publish` class, in which
        it can be defined in which topics should the return value be published.

        >>> class Order(BaseModel):
        ...     id: int
        ...     name: str
        ...     price: float
        ...
        >>> @app.subscriber(topic="topic")
        ... def subscriber_func(
        ...     order: Order = JSONMessage(),
        ... ) -> Publish(to_topics=["discounted_orders"], message=JSONMessage()):
        ...    order.price *= 0.9
        ...    return order

        Args:
            topic: The topic to subscribe to.
            on_deserialization_error: The function to call when a `flowtastic.message.DeserializationError`
                occurs. If not provided, then the error will be ignored.
            on_validation_error: The function to call when a `pydantic.ValidationError`
                occurs. If not provided, then the error will be ignored.

        Raises:
            ValueError: If the function is not a coroutine (async function), if the function
                has more than one parameter or if the function return type is not a `Publish`
                class instance.
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
            (
                func_input_type,
                func_input_default,
                func_output_type,
            ) = _get_subscriber_func_info(func)
            if (
                not isinstance(func_output_type, Publish)
                and func_output_type is not None
            ):
                raise ValueError(
                    f"The return type of the subscriber function must be an instance of "
                    f"`{Publish.__name__}`. You're probably seeing this error because "
                    f"one of the function that you have decorated using "
                    f"`@{self.__class__.__name__}.subscribe` method has a return type "
                    f"different from `{Publish.__name__}`."
                )
            if not func_input_default:
                logger.debug(
                    f"Subscriber func {func} did not have default value. Using `JSONMessage`..."
                )
                func_input_default = JSONMessage()
            deserializer: Message | _DeserializationCombo
            if is_base_model_subclass(func_input_type):
                logger.debug(
                    f"Registering {func} as subscriber for '{topic}' topic with deserialization "
                    f"of `{func_input_default}` to `{func_input_type}` `pydantic.BaseModel`"
                )
                deserializer = _DeserializationCombo(
                    message=func_input_default,
                    pydantic_base_model=func_input_type,  # type: ignore
                )
            else:
                logger.debug(
                    f"Registering {func} as subscriber for '{topic}' topic with deserialization "
                    f"of `{func_input_default}`"
                )
                deserializer = func_input_default
            self._register_subscriber(
                topic=topic,
                func=func,
                deserializer=deserializer,
                publish=func_output_type,
            )
            return func

        return register_subscriber

    def _consumed_topics(self) -> list[str]:
        """Returns the topics that should be consumed by the application.

        Returns:
            A list of topics to be consumed.
        """
        return list(self._topic_to_deserializer.keys())

    async def _create_consumer(self) -> None:
        """Creates and starts an instance of `aiokafka.AIOKafkaConsumer` to consume messages
        from the Kafka Broker"""
        self._consumer = AIOKafkaConsumer(
            *self._consumed_topics(), bootstrap_servers=self.broker
        )
        await self._consumer.start()

    async def _create_producer(self) -> None:
        """Creates an instance of `aiokafka.AIOKafkaProducer` to publish messages to the
        Kafka Broker."""
        self._producer = AIOKafkaProducer(bootstrap_servers=self.broker)
        await self._producer.start()

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

    async def _publish(self, python_object: BaseModel | Any, publish: Publish) -> None:
        """Publishes the `python_object` serializing it to a `bytes` object using the
        `publish.message` message serializer and then publishing it to the `publish.to_topics`.

        Args:
            python_object: The object to be serialized and published.
            publish: An instance of `Publish` containing the message serializer and the
                destination topics.
        """
        message = publish.message.serialize(python_object)
        to_be_awaited = []
        for topic in publish.to_topics:
            to_be_awaited.append(self._producer.send_and_wait(topic, message))
        asyncio.gather(*to_be_awaited)

    async def _subscriber_wrapper(
        self, func: SubscriberFunc, arg: BaseModel | Any
    ) -> None:
        """Wraps the subscriber function to handle the return value of the function if needed.

        Args:
            func: The subscriber function to wrap.
            arg: The argument to pass to the subscriber function.
        """
        result = await func(arg)
        publish = self._subscriber_to_publish.get(func, None)
        if publish and result:
            await self._publish(python_object=result, publish=publish)

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
            if python_object:
                subscribers = common(
                    self._topic_to_subscriber[topic],
                    self._deserializer_to_subscriber[deserializer],
                )
                to_be_awaited.append(
                    create_gathering(
                        subscribers, self._subscriber_wrapper, python_object
                    )
                )
        if to_be_awaited:
            asyncio.gather(*to_be_awaited)

    async def _consume(self) -> None:
        """Start consuming messages from the Kafka broker."""
        await self._create_consumer()
        await self._create_producer()
        try:
            async for msg in self._consumer:
                logger.debug(f"Distributing message from topic '{msg.topic}'...")
                await self._distribute_message(msg)
        finally:
            await self._consumer.stop()

    async def _stop(self) -> None:
        await self._consumer.stop()
        await self._producer.stop()

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
            logger.debug("Stopping FlowTastic application...")
            self._loop.run_until_complete(self._stop())
            self._loop.close()
