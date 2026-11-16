"""Tiffany plugin API 1 entry; the host supplies plugins.repeater and config."""
from pathlib import Path

from core import APP_PATHS, ServiceKey
from fields import GROUP_ID, MESSAGE_ID, MESSAGE_TYPE, SELF_ID, USER_ID

from .config import RepeaterConfig
from .service import RepeaterService

SERVICE = ServiceKey[RepeaterService]("business.repeater")


def install(scope, config) -> None:
    """Register resources on the host-owned Scope without starting any I/O."""
    if isinstance(config, dict):
        config = RepeaterConfig.from_mapping(config)
    if not isinstance(config, RepeaterConfig):
        raise TypeError("expected RepeaterConfig or a configuration mapping")
    paths = scope.bot.services.get(APP_PATHS)
    router = None
    dependencies = [APP_PATHS]
    # Respect an installed native permissions/commands service when available.
    from hooks.extensions.commands import ROUTER
    try:
        router = scope.bot.services.get(ROUTER)
    except LookupError:
        pass
    else:
        dependencies.append(ROUTER)
    service = RepeaterService(Path(paths.storage) / "repeater", config, scope.owner, router,
                              config_path=Path(paths.config).with_name("repeater.toml"))
    scope.service(SERVICE, service, dependencies=tuple(dependencies))
    scope.register_hook(service.handle, name="repeater.group", on="message", priority=100,
                        needs=(MESSAGE_TYPE, GROUP_ID, USER_ID, SELF_ID, MESSAGE_ID), uses=(SERVICE,))
    scope.register_hook(service.handle_notice, name="repeater.feedback", on="notice", priority=100, uses=(SERVICE,))


__all__ = ["SERVICE", "RepeaterConfig", "install"]
