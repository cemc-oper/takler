"""Private in-memory identities for the controlled RUN boundary."""

from types import FunctionType


def class_members(cls):
    return {
        name: value
        for base in reversed(cls.__mro__)
        if base is not object
        for name, value in vars(base).items()
        # copyreg caches slot discovery on ordinary classes during deepcopy.
        if not (
            name == "__slotnames__"
            and type(value) is list
            and all(type(item) is str for item in value)
        )
    }


class _Identity:
    """Keep originals alive and compare without invoking an extension's __eq__."""

    __slots__ = ("value",)

    def __init__(self, value):
        self.value = value

    def __eq__(self, other):
        return type(other) is _Identity and self.value is other.value


def stamp(value, parents=()):
    if type(value) in (staticmethod, classmethod):
        value = value.__func__
    if type(value) is property:
        return (_Identity(value), stamp(value.fget), stamp(value.fset))
    if type(value) is FunctionType:
        if any(value is parent for parent in parents):
            return ("cyclic_wrapper",)
        attributes = value.__dict__
        if any(type(name) is not str for name in attributes):
            return ("non_string_function_attribute",)
        metadata = (
            tuple(
                (
                    name,
                    stamp(item, (*parents, value))
                    if name == "__wrapped__"
                    else (_Identity(item),),
                )
                for name, item in attributes.items()
            )
            if attributes
            else ()
        )
        return (metadata,) + tuple(
            _Identity(part)
            for part in (
                value,
                value.__code__,
                value.__defaults__,
                value.__kwdefaults__,
            )
        )
    return (_Identity(value),)


def fingerprint(cls):
    return {name: stamp(value) for name, value in class_members(cls).items()}
