"""Run asset_store_server api."""
import logging
import threading

import connexion
from flask import Flask
from flask_injector import FlaskInjector

from proof_of_concept.asset_store_server.store import AssetStore

logger = logging.getLogger(__file__)


def _configure_dependency_injection(flask_app: Flask, store: AssetStore
                                    ) -> None:
    """Configure dependency injection.
    (https://levelup.gitconnected.com/ \
    python-dependency-injection-with-flask-injector-50773d451a32)

    Args:
        flask_app: The flask app
        store: asset store object to be injected

    """
    def configure(binder):
        binder.bind(AssetStore, to=store)
    FlaskInjector(
        app=flask_app,
        modules=[configure],
    )


def create_app(store: AssetStore) -> Flask:
    """Create app.

    Args:
        store: The assetstore where we store assets

    Returns:
        flask app

    """
    app = connexion.FlaskApp(__name__,
                             specification_dir='openapi/',
                             debug=True)
    app.add_api('openapi.yml',
                options={"swagger_ui": True},
                arguments={'title': 'Proof of concept Asset store'}
                )
    _configure_dependency_injection(app.app, store)

    return app.app


def run(port: int = 5000, **kwargs) -> None:
    """Run asset store server.

    Args:
        port: Port to run on
        **kwargs: kwargs for app.run

    """
    store = AssetStore('asset_store')
    app = create_app(store)
    app.run(port=port, **kwargs)


def run_async(port: int = 5000):
    """Run app in background thread."""
    def _run():
        run(port=port, use_reloader=False)
    threading.Thread(target=_run).start()


if __name__ == "__main__":
    run(debug=True)
