"""Storage and exchange of data and compute assets."""
from copy import copy
import logging
from pathlib import Path
from shutil import copyfile, move, rmtree
from tempfile import mkdtemp
from typing import Dict, Optional

from proof_of_concept.definitions.assets import (
        Asset, ComputeAsset, DataAsset)
from proof_of_concept.definitions.identifier import Identifier
from proof_of_concept.definitions.interfaces import IAssetStore
from proof_of_concept.policy.evaluation import (
        PermissionCalculator, PolicyEvaluator)


logger = logging.getLogger(__name__)


class AssetStore(IAssetStore):
    """A simple store for assets."""
    def __init__(
            self, policy_evaluator: PolicyEvaluator,
            image_dir: Optional[Path] = None) -> None:
        """Create a new empty AssetStore."""
        self._policy_evaluator = policy_evaluator
        self._permission_calculator = PermissionCalculator(policy_evaluator)
        self._assets = dict()  # type: Dict[Identifier, Asset]
        if image_dir is None:
            image_dir = Path(mkdtemp())
        self._image_dir = image_dir

    def close(self) -> None:
        """Releases resources, call when done."""
        rmtree(self._image_dir, ignore_errors=True)

    def store(self, asset: Asset, move_image: bool = False) -> None:
        """Stores an asset.

        Args:
            asset: asset object to store
            move_image: If the asset has an image and True is passed,
                the image file will be moved rather than copied into
                the store.

        Raises:
            KeyError: If there's already an asset with the asset id.

        """
        if asset.id in self._assets:
            raise KeyError(f'There is already an asset with id {id}')

        self._assets[asset.id] = copy(asset)
        if asset.image_location is not None:
            src_path = Path(asset.image_location)
            tgt_path = self._image_dir / f'{asset.id}.tar.gz'
            if move_image:
                move(str(src_path), str(tgt_path))
            else:
                copyfile(src_path, tgt_path)
            self._assets[asset.id].image_location = str(tgt_path)

    def store_image(
            self, asset_id: Identifier, image_file: Path,
            move_image: bool = False) -> None:
        """Stores an image for an already-stored asset.

        Args:
            asset_id: ID of the asset to add an image for.
            move: Whether to move the input file rather than copy it.

        Raises:
            KeyError: If there's no asset with the given ID.

        """
        if asset_id not in self._assets:
            raise KeyError(f'Asset with id {asset_id} not found.')

        asset = self._assets[asset_id]
        tgt_path = self._image_dir / f'{asset_id}.tar.gz'
        if move_image:
            move(str(image_file), str(tgt_path))
        else:
            copyfile(image_file, tgt_path)
        asset.image_location = str(tgt_path)

    def retrieve(self, asset_id: Identifier, requester: Identifier) -> Asset:
        """Retrieves an asset.

        Args:
            asset_id: ID of the asset to retrieve.
            requester: Name of the site making the request.

        Return:
            The asset object with asset_id.

        Raises:
            KeyError: If no asset with the given id is stored here.

        """
        logger.info(f'{self}: servicing request from {requester} for data: '
                    f'{asset_id}')
        if asset_id not in self._assets:
            msg = (
                    f'{self}: Asset {asset_id} not found'
                    f' (requester = {requester}).')
            logger.info(msg)
            raise KeyError(msg)

        asset = self._assets[asset_id]

        if isinstance(asset, DataAsset):
            perms = self._permission_calculator.calculate_permissions(
                    asset.metadata.job)
            perm = perms[asset.metadata.item]

        if isinstance(asset, ComputeAsset):
            perm = self._policy_evaluator.permissions_for_asset(asset_id)

        if not self._policy_evaluator.may_access(perm, requester):
            raise RuntimeError(f'{self}: Security error, access denied'
                               f'for {requester} to {asset_id}')
        logger.info(f'{self}: Sending asset {asset_id} to {requester}')
        return asset
