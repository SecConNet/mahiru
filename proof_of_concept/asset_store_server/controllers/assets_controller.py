"""Controllers for asset store server 'assets' endpoint."""
import json
from typing import Any

import connexion  # type: ignore
from flask import abort

from proof_of_concept.asset import Asset
from proof_of_concept.asset_store_server.db import db


def get_asset(asset_id: str, requester: str) -> Any:
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


def store_asset() -> Any:
    """Store an asset."""
    if connexion.request.is_json:
        asset = Asset.from_dict(connexion.request.get_json())
        db[asset.id] = asset
        return None, 201
    else:
        abort(400, 'Please pass json')
