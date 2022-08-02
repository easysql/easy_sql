import inspect
import os.path
import re

from easy_sql.sql_processor import funcs_spark, funcs_rdb


def _render_doc_modules_functions(backend: str):
    print('render doc for:', backend)
    assert backend in ['spark', 'rdb']
    mod = funcs_spark if backend == 'spark' else funcs_rdb
    groups_doc = []
    for funcs_group in mod.__all__:
        mod_name: str = funcs_group
        funcs_group_mod = getattr(mod, funcs_group)
        funcs = [func for func in dir(funcs_group_mod)
                 if not func.startswith('_') and func == func.lower()]
        assert mod_name.endswith('Func') or mod_name.endswith('Funcs') or mod_name.endswith('Functions')
        group_name = mod_name[:mod_name.rindex('Func')]

        funcs_doc = []
        for func_name in funcs:
            func_mod = getattr(funcs_group_mod, func_name)
            func_sig = str(inspect.signature(func_mod)).replace('(self, ', '(', 1)
            module = func_mod.__module__
            func_doc = f'- [`{func_name}{func_sig}`]' \
                       f'(https://easy-sql.readthedocs.io/en/latest/autoapi/{module.replace(".", "/")}/index.html#{module}.{mod_name}.{func_name})'
            funcs_doc.append(func_doc)
        funcs_doc = "\n".join(funcs_doc)

        funcs_group_doc = f'''
#### {group_name} functions

{funcs_doc}
'''
        groups_doc.append(funcs_group_doc)
    return "\n".join(groups_doc)


def _update_function_doc(doc_tpl_file: str, doc_file: str):
    with open(doc_tpl_file, 'r') as f:
        doc_tpl = f.read()
    lines = doc_tpl.split('\n')
    result_lines = []
    for line in lines:
        m = re.match(r'\{\{ (spark|rdb) functions:? ?(.*)? \}\}', line)
        if m:
            groups = m.groups()
            backend = groups[0]
            title = groups[1].strip() if len(groups) > 1 and groups[1].strip() else f'Functions for {backend} backend'
            doc = f'''
### {title}

{_render_doc_modules_functions(backend)}
'''
            result_lines.append(doc)
        else:
            result_lines.append(line)

    with open(doc_file, 'w') as f:
        f.write('\n'.join(result_lines))
        print('updated file:', doc_file)


def update_func_doc():
    doc_tpl_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../easy_sql/functions.tpl.md')
    doc_file = os.path.join(os.path.dirname(os.path.abspath(__file__)), '../easy_sql/functions.md')
    _update_function_doc(doc_tpl_file, doc_file)


if __name__ == '__main__':
    update_func_doc()
