"""Identifier for various things in the DDM."""
import re
from typing import Any, cast, List, Type


class Identifier(str):
    """An identifier.

    An Identifier is a string of any of the following forms:

    1. party:<namespace>:<name>
    2. party_category:<namespace>:<name>
    3. site:<namespace>:<name>
    4. site_category:<namespace>:<name>
    5. asset:<namespace>:<name>:<site_namespace>:<site_name>
    6. asset_collection:<namespace>:<name>
    7. asset_category:<namespace>:<name>
    8. result:<id_hash>

    This class also accepts a single asterisk as an identifier, as it
    is used as a wildcard in rules.

    See the Terminology section of the documentation for details.
    """
    def __new__(cls: Type['Identifier'], seq: Any) -> 'Identifier':
        """Create an Identifier.

        Args:
            seq: Contents, will be converted to a string using str(),
            then used as the identifier.

        Raises:
            ValueError: If str(seq) is not a valid identifier.
        """
        data = str(seq)

        if data != '*':
            segments = data.split(':')
            if segments[0] not in cls._kinds:
                raise ValueError(f'Invalid identifier kind {segments[0]}')

            if len(segments) != cls._lengths[segments[0]]:
                raise ValueError(f'Too few or too many segments in {data}')

            for segment in segments:
                if not cls._segment_regex.match(segment):
                    raise ValueError(f'Invalid identifier segment {segment}')

        return str.__new__(cls, seq)

    @classmethod
    def from_id_hash(cls, id_hash: str) -> 'Identifier':
        """Creates an Identifier from an id hash.

        Args:
            id_hash: A hash of a workflow that created this result.

        Returns:
            The Identifier for the workflow result.
        """
        return cls(f'result:{id_hash}')

    @property
    def segments(self) -> List[str]:
        """Return the list of segments of this identifier."""
        return self.split(':')

    def namespace(self) -> str:
        """Returns the namespace part of the identifier.

        Raises:
            RuntimeError: If this is the identifier of a result, which
                don't have namespaces.
        """
        if self.segments[0] == 'result':
            raise RuntimeError('Results do not have a namespace')
        return self.segments[1]

    def name(self) -> str:
        """Returns the name part of the identifier.

        Raises:
            RuntimeError: If this is the identifier of a result, which
                don't have namespaces.
        """
        if self.segments[0] == 'result':
            raise RuntimeError('Results do not have a name')
        return self.segments[2]

    def location(self) -> 'Identifier':
        """Returns the identifier of the site storing this asset.

        Returns:
            A site name.

        Raises:
            RuntimeError: If this is not a concrete asset.
        """
        if self.segments[0] != 'asset':
            raise RuntimeError(
                    'Location requested of non-concrete asset {self}')
        return Identifier(f'site:{self.segments[3]}:{self.segments[4]}')

    _kinds = (
        'party', 'party_category', 'site', 'site_category', 'asset',
        'asset_collection', 'asset_category', 'result')

    _lengths = {
            'party': 3, 'party_category': 3, 'site': 3, 'site_category': 3,
            'asset': 5, 'asset_collection': 3, 'asset_category': 3,
            'result': 2}

    _segment_regex = re.compile('[a-zA-Z0-9_.-]*')
