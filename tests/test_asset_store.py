from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mahiru.components.asset_store import AssetStore
from mahiru.definitions.assets import DataAsset
from mahiru.definitions.identifier import Identifier


def test_asset_store_store_retrieve(image_dir, test_image_file) -> None:
    mock_policy_evaluator = MagicMock()
    store = AssetStore(mock_policy_evaluator, image_dir)

    asset_id = Identifier('asset:ns:test_asset:ns:site')
    asset = DataAsset(asset_id, None, str(test_image_file))
    store.store(asset)

    asset2 = store.retrieve(asset_id, MagicMock())
    assert asset2.image_location == str(image_dir / f'{asset_id}.tar.gz')

    assert test_image_file.exists()
    with (image_dir / f'{asset_id}.tar.gz').open('r') as f:
        assert f.read() == 'testing'


def test_asset_store_store_move(image_dir, test_image_file) -> None:
    mock_policy_evaluator = MagicMock()
    store = AssetStore(mock_policy_evaluator, image_dir)

    asset = DataAsset(
            'asset:ns:test_asset:ns:site', None, str(test_image_file))
    store.store(asset, True)

    assert not test_image_file.exists()
    with (image_dir / 'asset:ns:test_asset:ns:site.tar.gz').open('r') as f:
        assert f.read() == 'testing'
