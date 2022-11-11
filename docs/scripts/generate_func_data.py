from __future__ import annotations

import inspect
import json
import os.path
from typing import Dict, Optional

from easy_sql.sql_processor import funcs_rdb, funcs_spark
from easy_sql.sql_processor.funcs import FuncRunner


class FuncDoc:
    def __init__(self, label: str, type: str) -> None:
        self.label = label
        self.type = type

    @staticmethod
    def from_sig_str(func_name: str, sig: Optional[inspect.Signature] = None, type: str = "easysql") -> FuncDoc:
        if sig is None:
            return FuncDoc(func_name + "()", type)

        parameters = list(sig.parameters.items())

        if not parameters:
            return FuncDoc(func_name + "()", type)

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

        return FuncDoc(func_name + "(" + ", ".join(args_str) + ")", type)

    def as_dict(self) -> Dict:
        return {"label": self.label, "type": self.type}


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
            func_docs.append(FuncDoc.from_sig_str(func_name, func_sig))

    for func_name, func in FuncRunner.system_funcs().items():
        try:
            sig = inspect.signature(func)
            func_docs.append(FuncDoc.from_sig_str(func_name, sig, "system"))
        except ValueError:
            func_docs.append(FuncDoc.from_sig_str(func_name))
        except Exception:
            print("ignore unparsable function: " + func_name)
            pass

    generated_file_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))),
        f"build/{backend}.json",
    )
    with open(generated_file_path, "w") as f:
        f.write(json.dumps({"funcs": [doc.as_dict() for doc in func_docs]}, indent=4, ensure_ascii=False))
        print("generated file: ", generated_file_path)


if __name__ == "__main__":
    generate_doc("spark")
    generate_doc("rdb")
