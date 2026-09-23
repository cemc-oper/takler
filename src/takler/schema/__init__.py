"""Pure data contracts, independent of execution objects."""

from .definition import DefinitionDocument, DefinitionError, parse_definition

__all__ = ["DefinitionDocument", "DefinitionError", "parse_definition"]
