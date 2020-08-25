import connexion
from flask import abort
from injector import inject

from proof_of_concept.asset_store import AssetStore
from proof_of_concept.asset_store_server.models.asset import Asset


@inject
def get_asset(store: AssetStore, asset_id: str, requester: str):
    """Retrieve an asset by ID.

    Arguments:
        store: Asset store
        asset_id: The id of the asset to retrieve
        requester: The id of the requester
    """
    try:
        asset = store.retrieve(asset_id, requester)
    except KeyError:
        abort(404)
    else:
        # TODO: Building the (Flask) asset based on the retrieved asset
        #  object is a nightmare, maybe merge all the Flask models with our
        #  own classes?
        Asset()
        return asset


def store_asset(body=None):
    """Store an asset

     # noqa: E501

    :param body:
    :type body: dict | bytes

    :rtype: None
    """
    if connexion.request.is_json:
        body = Asset.from_dict(connexion.request.get_json())  # noqa: E501

    # TODO: Same conversion nightmare, and also not sure whether we add a
    #  DataAsset or ComputeAsset
    return 'do some magic!'
