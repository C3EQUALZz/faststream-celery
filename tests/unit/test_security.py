import ssl

import pytest
from faststream.security import (
    SASLGSSAPI,
    BaseSecurity,
    SASLPlaintext,
    SASLScram256,
)

from faststream_celery import CeleryBroker
from faststream_celery.security import parse_security


class TestParseSecurity:
    def test_no_security_adds_nothing(self) -> None:
        assert parse_security(None) == {}

    def test_plain_security_without_ssl_adds_nothing(self) -> None:
        assert parse_security(BaseSecurity(use_ssl=False)) == {}

    def test_use_ssl_turns_ssl_on(self) -> None:
        assert parse_security(BaseSecurity(use_ssl=True)) == {"ssl": True}

    def test_an_ssl_context_is_refused(self) -> None:
        """Kombu wants its own ssl options, not an `ssl.SSLContext`."""
        context = ssl.create_default_context()

        with pytest.raises(NotImplementedError, match="cannot take an `ssl_context`"):
            parse_security(BaseSecurity(ssl_context=context))

    def test_sasl_plaintext_becomes_kombu_credentials(self) -> None:
        assert parse_security(SASLPlaintext(username="user", password="pass")) == {
            "userid": "user",
            "password": "pass",
        }

    def test_sasl_plaintext_can_also_carry_ssl(self) -> None:
        assert parse_security(
            SASLPlaintext(username="user", password="pass", use_ssl=True),
        ) == {
            "ssl": True,
            "userid": "user",
            "password": "pass",
        }

    @pytest.mark.parametrize(
        "security",
        (
            SASLScram256(username="user", password="pass"),
            SASLGSSAPI(),
        ),
    )
    def test_unsupported_mechanisms_are_refused(self, security: BaseSecurity) -> None:
        with pytest.raises(NotImplementedError, match="CeleryBroker does not support"):
            parse_security(security)


class TestSecurityOnTheBroker:
    def test_credentials_reach_the_connection(self) -> None:
        broker = CeleryBroker(
            "amqp://localhost:5672//",
            security=SASLPlaintext(username="user", password="pass"),
        )

        connection = broker.config.broker_config.make_connection()

        assert connection.userid == "user"
        assert connection.password == "pass"

    def test_an_explicit_ssl_argument_wins_over_security(self) -> None:
        """`ssl=` is the kombu-level escape hatch, so it takes precedence."""
        broker = CeleryBroker(
            "amqp://localhost:5672//",
            ssl=False,
            security=BaseSecurity(use_ssl=True),
        )

        assert broker.config.broker_config.make_connection().ssl is False

    def test_security_ssl_is_used_when_no_argument_is_given(self) -> None:
        broker = CeleryBroker(
            "amqp://localhost:5672//",
            security=BaseSecurity(use_ssl=True),
        )

        assert broker.config.broker_config.make_connection().ssl is True

    def test_the_specification_records_the_security(self) -> None:
        security = SASLPlaintext(username="user", password="pass")
        broker = CeleryBroker("amqp://localhost:5672//", security=security)

        assert broker.specification.security is security
