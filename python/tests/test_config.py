import pytest

from discordia_pubsub.config import Config, InvalidConfigError, load_config, parse_duration


def test_defaults():
    config = load_config({"SERVICE_NAME": "identity"})

    assert config.url == "amqp://localhost:5672/"
    assert config.exchange == "discordia.events"
    assert config.dead_letter_exchange == "discordia.events.dlx"
    assert config.service_name == "identity"
    assert config.prefetch == 16
    assert config.max_retries == 3
    assert config.retry_initial_delay == pytest.approx(0.2)
    assert config.retry_max_delay == pytest.approx(5.0)
    assert config.publish_timeout == pytest.approx(5.0)
    assert config.reconnect_initial_delay == pytest.approx(0.5)
    assert config.reconnect_max_delay == pytest.approx(30.0)


def test_overrides_from_the_environment():
    config = load_config(
        {
            "RABBITMQ_URL": "amqp://user:pass@rabbitmq:5672/discordia",
            "RABBITMQ_EXCHANGE": "discordia.events.test",
            "RABBITMQ_DEAD_LETTER_EXCHANGE": "discordia.dead",
            "SERVICE_NAME": "community",
            "PUBSUB_PREFETCH": "32",
            "PUBSUB_MAX_RETRIES": "5",
            "PUBSUB_RETRY_INITIAL_DELAY": "1s",
            "PUBSUB_RETRY_MAX_DELAY": "10s",
            "PUBSUB_PUBLISH_TIMEOUT": "2s",
            "PUBSUB_RECONNECT_INITIAL_DELAY": "100ms",
            "PUBSUB_RECONNECT_MAX_DELAY": "1m",
        }
    )

    assert config.url == "amqp://user:pass@rabbitmq:5672/discordia"
    assert config.exchange == "discordia.events.test"
    assert config.dead_letter_exchange == "discordia.dead"
    assert config.prefetch == 32
    assert config.max_retries == 5
    assert config.retry_initial_delay == pytest.approx(1.0)
    assert config.reconnect_max_delay == pytest.approx(60.0)


def test_dead_letter_exchange_follows_the_exchange():
    config = load_config({"SERVICE_NAME": "mod", "RABBITMQ_EXCHANGE": "discordia.events.ci"})

    assert config.dead_letter_exchange == "discordia.events.ci.dlx"


def test_empty_value_falls_back_to_the_default():
    config = load_config({"SERVICE_NAME": "identity", "PUBSUB_PREFETCH": ""})

    assert config.prefetch == 16


@pytest.mark.parametrize(
    "key,value",
    [
        ("PUBSUB_PREFETCH", "muchos"),
        ("PUBSUB_MAX_RETRIES", "3.5"),
        ("PUBSUB_RETRY_INITIAL_DELAY", "200"),
        ("PUBSUB_RETRY_MAX_DELAY", "un rato"),
        ("PUBSUB_PUBLISH_TIMEOUT", "5 s"),
        ("PUBSUB_RECONNECT_INITIAL_DELAY", "x"),
        ("PUBSUB_RECONNECT_MAX_DELAY", ""),
    ],
)
def test_malformed_values_fail_instead_of_falling_back(key, value):
    env = {"SERVICE_NAME": "identity", key: value}

    if value == "":
        # Vacío sí cae al default: es lo que pasa con una variable declarada
        # y sin valor en un .env.
        assert load_config(env).reconnect_max_delay == pytest.approx(30.0)
        return

    with pytest.raises(InvalidConfigError, match=key):
        load_config(env)


def test_service_name_is_required():
    with pytest.raises(InvalidConfigError, match="SERVICE_NAME"):
        load_config({})


# El formato de duración es el de Go para que el mismo .env valga en los dos
# lenguajes.
@pytest.mark.parametrize(
    "raw,expected",
    [("200ms", 0.2), ("5s", 5.0), ("1m", 60.0), ("1h", 3600.0), ("1m30s", 90.0), ("1.5s", 1.5), ("500us", 0.0005)],
)
def test_parses_go_style_durations(raw, expected):
    assert parse_duration(raw) == pytest.approx(expected)


@pytest.mark.parametrize("raw", ["200", "", "5 s", "cinco", "5x", "s5"])
def test_rejects_malformed_durations(raw):
    with pytest.raises(InvalidConfigError):
        parse_duration(raw)


def _valid_config(**overrides) -> Config:
    base = {
        "url": "amqp://localhost:5672/",
        "exchange": "discordia.events",
        "dead_letter_exchange": "discordia.events.dlx",
        "service_name": "identity",
    }
    return Config(**{**base, **overrides})


@pytest.mark.parametrize(
    "overrides",
    [
        {"url": ""},
        {"exchange": ""},
        {"dead_letter_exchange": ""},
        {"dead_letter_exchange": "discordia.events"},
        {"service_name": ""},
        {"prefetch": 0},
        {"max_retries": -1},
        {"retry_initial_delay": 0},
        {"retry_max_delay": 0.001},
        {"publish_timeout": 0},
        {"reconnect_initial_delay": 0},
        {"reconnect_max_delay": 0.001},
    ],
)
def test_validate_rejects_inconsistent_values(overrides):
    with pytest.raises(InvalidConfigError):
        _valid_config(**overrides).validate()


def test_zero_retries_is_valid():
    config = load_config({"SERVICE_NAME": "metrics", "PUBSUB_MAX_RETRIES": "0"})

    assert config.max_retries == 0
