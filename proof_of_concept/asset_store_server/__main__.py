import connexion
from flask_injector import FlaskInjector

from proof_of_concept.asset_store import AssetStore


def _configure_dependency_injection(flask_app, store: AssetStore) -> None:
    def configure(binder):
        binder.bind(AssetStore, to=store)
    FlaskInjector(
        app=flask_app,
        modules=[configure],
    )


def create_app(store: AssetStore):
    app = connexion.FlaskApp(__name__, specification_dir='openapi/', debug=True)
    app.add_api('openapi.yml',
                options={"swagger_ui": True},
                arguments={'title': 'Proof of concept Asset store'}
                )
    _configure_dependency_injection(app.app, store)

    return app.app


def main():
    # TODO: Pass policy evaluator
    store = AssetStore('asset_store')
    app = create_app(store)
    app.run(debug=True)


if __name__ == "__main__":
    main()

