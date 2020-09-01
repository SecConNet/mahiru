"""Run asset_store_server api."""
import logging
import threading
from typing import Any, Dict

import connexion

logger = logging.getLogger(__file__)


def create_app() -> Any:
    """Create app.

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
    return app.app


def run(port: int = 5000, **kwargs: int) -> None:
    """Run asset store server.

    Args:
        port: Port to run on
        **kwargs: kwargs for app.run

    """
    app = create_app()
    app.run(port=port, **kwargs)


def run_async(port: int = 5000) -> None:
    """Run app in background thread."""
    def _run() -> None:
        run(port=port, use_reloader=False)
    thread = threading.Thread(target=_run)
    thread.daemon = True
    thread.start()


if __name__ == "__main__":
    run(debug=True)
