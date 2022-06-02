from __future__ import annotations

from typing import TYPE_CHECKING

from flowtastic.message import JSONMessage

if TYPE_CHECKING:
    from flowtastic.message import Message


class Publish:
    """A class used to indicate that the return value of a subscriber function should be
    published to the specified topics.

    Attributes:
        to_topics: The list of topics to publish to.
        message: The `Message` class instance used to serialize the return value of the
            subscriber function before publishing it to the topics.
    """

    to_topics: list[str]
    message: Message

    def __init__(self, to_topics: list[str], message: Message = JSONMessage()) -> None:
        """Inits `Publish` class.

        Args:
            to_topics: The list of topics to publish to.
            message: The `Message` class instance used to serialize the return value of the
                subscriber function before publishing it to the topics.
        """
        self.to_topics = to_topics
        self.message = message
