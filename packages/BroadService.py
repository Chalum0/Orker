from collections.abc import Mapping
from typing import get_type_hints
from functools import wraps
import inspect


class BroadService:
    json_key = "data"

    _hidden_methods = {
        "returns",
        "help",
    }

    @property
    def _service_type(self):
        for base in self.__class__.__bases__:
            if base.__name__ == "Gateway":
                return "gateway"
        return "service"

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)

        for name, value in list(cls.__dict__.items()):
            if name.startswith("_"):
                continue

            if name in cls._hidden_methods:
                continue

            if name.endswith("_json"):
                continue

            if inspect.isfunction(value):
                cls._add_json_method(name, value)

    import inspect
    from functools import wraps

    @classmethod
    def _add_json_method(cls, name, func):
        json_name = f"{name}_json"

        if hasattr(cls, json_name):
            return

        sig = inspect.signature(func)

        # skip self, because wrapper already passes self manually
        params = list(sig.parameters.values())[1:]

        has_varargs = any(
            p.kind == inspect.Parameter.VAR_POSITIONAL
            for p in params
        )

        has_varkwargs = any(
            p.kind == inspect.Parameter.VAR_KEYWORD
            for p in params
        )

        positional_params = [
            p for p in params
            if p.kind in (
                inspect.Parameter.POSITIONAL_ONLY,
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
            )
        ]

        allowed_kwargs = {
            p.name for p in params
            if p.kind in (
                inspect.Parameter.POSITIONAL_OR_KEYWORD,
                inspect.Parameter.KEYWORD_ONLY,
            )
        }

        def clean_call_args(args, kwargs):
            args = tuple(args)
            kwargs = dict(kwargs)

            # too many positional args → cut
            if not has_varargs:
                args = args[:len(positional_params)]

            # unknown kwargs → remove
            if not has_varkwargs:
                kwargs = {
                    k: v
                    for k, v in kwargs.items()
                    if k in allowed_kwargs
                }

            # avoid: got multiple values for argument 'x'
            filled_by_position = {
                p.name
                for p in positional_params[:len(args)]
                if p.kind == inspect.Parameter.POSITIONAL_OR_KEYWORD
            }

            for key in filled_by_position:
                kwargs.pop(key, None)

            return args, kwargs

        if inspect.iscoroutinefunction(func):
            @wraps(func)
            async def wrapper(self, *args, __func=func, **kwargs):
                args, kwargs = clean_call_args(args, kwargs)
                result = await __func(self, *args, **kwargs)
                return self._to_json_result(result)
        else:
            @wraps(func)
            def wrapper(self, *args, __func=func, **kwargs):
                args, kwargs = clean_call_args(args, kwargs)
                result = __func(self, *args, **kwargs)
                return self._to_json_result(result)

        setattr(cls, json_name, wrapper)

    @classmethod
    def _to_json_result(cls, result):
        if isinstance(result, Mapping):
            return result

        if isinstance(result, bool):
            return {"condition": result}

        return {
            cls.json_key: result
        }

    @classmethod
    def _normalize_return_shape(cls, shape):
        if isinstance(shape, dict):
            return shape

        if shape == "bool":
            return {
                "condition": "bool"
            }

        return {
            cls.json_key: shape
        }

    @classmethod
    def _type_name(cls, tp):
        if tp is inspect._empty:
            return "any"

        origin = getattr(tp, "__origin__", None)
        args = getattr(tp, "__args__", ())

        if origin:
            args_str = ", ".join(cls._type_name(arg) for arg in args)
            return f"{cls._type_name(origin)}[{args_str}]"

        return getattr(tp, "__name__", str(tp))

    @classmethod
    def _get_method_params(cls, func):
        sig = inspect.signature(func)
        hints = get_type_hints(func)

        params = {}

        for name, param in sig.parameters.items():
            if name in ("self", "cls"):
                continue

            annotation = hints.get(name, param.annotation)

            params[name] = {
                "type": cls._type_name(annotation),
                "required": param.default is inspect._empty,
                "default": None if param.default is inspect._empty else param.default,
                "kind": param.kind.name,
            }

        return params

    def returns(self):
        return {}

    def help(self):
        manual = self.returns() or {}
        schema = {}

        for name, value in self.__class__.__dict__.items():
            if name.startswith("_"):
                continue

            if name in self._hidden_methods:
                continue

            if name.endswith("_json"):
                continue

            if not inspect.isfunction(value):
                continue

            raw_return_shape = manual.get(name, {}).get("returns", "unknown")
            display_params = manual.get(name, {}).get("display_params", False)

            schema[name] = {
                "params": self._get_method_params(value),
                "returns": self._normalize_return_shape(raw_return_shape),
                "service": self.__class__.__name__,
                "type": self._service_type,
                "display_params": display_params
            }
        return schema