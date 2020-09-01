"""Controllers for asset store server 'assets' endpoint."""
import json

import connexion
from flask import abort
from injector import inject

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store_server.db import db


@inject
def get_asset(asset_id: str, requester: str):
    """Retrieve an asset by ID.

    Arguments:
        asset_id: The id of the asset to retrieve
        requester: The id of the requester
    """
    try:
        asset = db[asset_id]
    except KeyError:
        abort(404, 'Asset not found')
    else:
        return json.dumps(asset.to_dict())


@inject
def store_asset():
    """Store an asset."""
    if connexion.request.is_json:
        asset = Asset.from_dict(connexion.request.get_json())  # noqa: E501
        db[asset.id] = asset
        return None, 201
    else:
        abort(400, 'Please pass json')
