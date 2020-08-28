import json

import connexion
from flask import abort
from injector import inject

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store import AssetStore


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
        abort(404, 'Asset not found')
    else:
        return json.dumps(asset.to_dict())


@inject
def store_asset(store: AssetStore):
    """Store an asset."""
    if connexion.request.is_json:
        asset = Asset.from_dict(connexion.request.get_json())  # noqa: E501
        store.store(asset)
        return None, 201
    else:
        abort(400, 'Please pass json')
