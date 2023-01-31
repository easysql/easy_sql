from __future__ import annotations

import inspect
import json
import os
import os.path
from typing import Dict, Optional

import jedi

from easy_sql.sql_processor import funcs_rdb, funcs_spark
from easy_sql.sql_processor.funcs import FuncRunner


class JediCompletion:
    @staticmethod
    def _generate_signature(completion):
        """Generate signature with function arguments."""
        if completion.type in ["module"] or not hasattr(completion, "params"):
            return ""
        return "%s(%s)" % (
            completion.name,
            ", ".join(p.description[6:] for p in completion.params if p),
        )

    @classmethod
    def serialize_tooltip(cls, script: jedi.Script):
        definitions = script.goto_definitions()
        _definitions = []
        for definition in definitions:
            signature = definition.name
            description = None
            if definition.type in ["class", "function"]:
                signature = cls._generate_signature(definition)
                try:
                    description = definition.docstring(raw=True).strip()
                except Exception:
                    description = ""
                if not description and not hasattr(definition, "get_line_code"):
                    # jedi returns an empty string for compiled objects
                    description = definition.docstring().strip()
            if definition.type == "module":
                signature = definition.full_name
                try:
                    description = definition.docstring(raw=True).strip()
                except Exception:
                    description = ""
                if not description and hasattr(definition, "get_line_code"):
                    # jedi returns an empty string for compiled objects
                    description = definition.docstring().strip()
            _definition = {
                "text": definition.name,
                "description": description,
                "docstring": description,
                "signature": signature,
            }
            _definitions.append(_definition)
        return _definitions[0] if len(_definitions) == 1 else {}


class FuncDoc:
    def __init__(self, label: str, type: str, tooltip: Dict) -> None:
        self.label = label
        self.type = type
        self.tooltip = tooltip
        if self.tooltip["signature"]:
            self.tooltip["signature"] = self.tooltip["signature"].replace("(self, ", "(")

    @staticmethod
    def from_sig_str(
        func_name: str,
        tooltip: Dict,
        sig: Optional[inspect.Signature] = None,
        type: str = "easysql",
    ) -> FuncDoc:
        if sig is None:
            return FuncDoc(func_name + "()", type, tooltip)

        parameters = list(sig.parameters.items())

        if not parameters:
            return FuncDoc(func_name + "()", type, tooltip)

        if parameters[0][0] == "self":
            parameters = parameters[1:]

        if any([p[1].kind == inspect.Parameter.VAR_KEYWORD for p in parameters]):
            raise Exception("var keyword argument (**kwargs) not supported: " + func_name + str(sig))

        end_var_arg_name = ""
        if parameters[-1][1].kind == inspect.Parameter.VAR_POSITIONAL:
            end_var_arg_name = parameters[-1][0]
            parameters = parameters[:-1]

        var_required_input_count = 1

        args_str = []
        for name, p in parameters:
            if p.annotation == "Backend":
                args_str.append("\\${__backend__}")
            elif p.annotation == "Step":
                args_str.append("\\${__step__}")
            elif p.annotation == "ProcessorContext":
                args_str.append("\\${__context__}")
            else:
                args_str.append(f"${{{var_required_input_count}:{name}}}")
                var_required_input_count += 1

        if end_var_arg_name:
            args_str.append(f"${{{var_required_input_count}:*{end_var_arg_name}}}")
            args_str.append(f"${{{var_required_input_count + 1}:...}}")

        return FuncDoc(func_name + "(" + ", ".join(args_str) + ")", type, tooltip)

    def as_dict(self) -> Dict:
        return {"label": self.label, "type": self.type, "tooltip": self.tooltip}


def generate_doc(backend: str):
    print("render doc for:", backend)
    assert backend in ["spark", "rdb"]

    mod = funcs_spark if backend == "spark" else funcs_rdb
    func_docs = []
    for funcs_group in mod.__all__:
        mod_name: str = funcs_group
        funcs_group_mod = getattr(mod, funcs_group)
        funcs = [func for func in dir(funcs_group_mod) if not func.startswith("_") and func == func.lower()]
        assert mod_name.endswith("Func") or mod_name.endswith("Funcs") or mod_name.endswith("Functions")

        for func_name in funcs:
            func_mod = getattr(funcs_group_mod, func_name)
            func_sig = inspect.signature(func_mod)
            tooltip = JediCompletion.serialize_tooltip(
                jedi.Script(
                    f"from easy_sql.sql_processor import funcs_{backend}; funcs_{backend}.{funcs_group}.{func_name}"
                )
            )
            func_docs.append(FuncDoc.from_sig_str(func_name, tooltip, func_sig))

    for func_name, func in FuncRunner.easysql_funcs().items():
        tooltip = JediCompletion.serialize_tooltip(
            jedi.Script(f"from from easy_sql.sql_processor import funcs; funcs.EASYSQL_FUNCS['{func_name}']")
        )
        sig = inspect.signature(func)
        func_docs.append(FuncDoc.from_sig_str(func_name, tooltip, sig))

    for func_name, func in FuncRunner.system_funcs().items():
        import builtins

        mod = "builtins" if hasattr(builtins, func_name) else "operator"
        tooltip = JediCompletion.serialize_tooltip(jedi.Script(f"import {mod}; {mod}.{func_name}"))
        try:
            sig = inspect.signature(func)
            func_docs.append(FuncDoc.from_sig_str(func_name, tooltip, sig, "system"))
        except ValueError:
            func_docs.append(FuncDoc.from_sig_str(func_name, tooltip, None, "system"))
        except Exception:
            print("ignore unparsable function: " + func_name)
            pass

    generated_file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        f"build/{backend}.json",
    )
    with open(generated_file_path, "w") as f:
        f.write(
            json.dumps(
                {"funcs": [doc.as_dict() for doc in func_docs]},
                indent=4,
                ensure_ascii=False,
            )
        )
        print("generated file: ", generated_file_path)


if __name__ == "__main__":
    generate_doc("spark")
    generate_doc("rdb")
