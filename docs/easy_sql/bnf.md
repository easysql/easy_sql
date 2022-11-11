The pseudocode BNF of Easy SQL syntax.

```
easysql: target_def | sql_body | config | include
target_def: target_def_prefix (variables_def | list_variables_def | temp_def | cache_def | broadcast_def | func_def | log_def | check_def | output_def | template_def | action) (, if = func_call)?
sql_body: (any var_reference any | any tpl_reference any)* comment?

target_def_prefix: '-- target='

var_reference: var_reference_lit | var_reference_func
var_reference_lit: ${ name }
var_reference_func: ${ func_call }

func_call: func_call_no_arg | func_call_with_args
func_call_no_arg: name \(  \)
func_call_with_args: name \( func_call_args \)
func_call_args: (name_wide | var_reference_lit) (, name_wide | , var_reference_lit)*

variables_def: 'variables'
list_variables_def: 'list_variables'
temp_def: 'temp.'name
cache_def: 'cache.'name
broadcast_def: 'broadcast.'name
func_def: 'func.'func_call
log_def: 'log.'name
check_def: 'check.'name | 'check.'func_call
output_def: 'output.'name.name | 'output.'name.name.name
template_def: 'template.'name
action_def: 'action.'name

config: '-- config:' name_key = any | \
    '-- backend:' name | \
    '-- owner:' | \
    '-- owner:' name (, name)* | \
    '-- schedule:' any | \
    '-- prepare-sql: ' any | \
    '-- inputs:' | \
    '-- inputs:' (name.name | name.name.name) (, name.name | , name.name.name)* | \
    '-- outputs:' | \
    '-- outputs:' (name.name | name.name.name) (, name.name | , name.name.name)*

include: '-- include=' any

tpl_call: tpl_call_no_arg | tpl_call_with_args
tpl_call_no_arg: name \(  \)
tpl_call_with_args: name \( tpl_call_args \)
tpl_call_args: (name = name_wide | name = var_reference_lit) (, name = name_wide | , name = var_reference_lit)*


tpl_reference: tpl_reference_lit | tpl_reference_func
tpl_reference_lit: @{ name }
tpl_reference_func: @{ tpl_call }

name: r'[a-zA-Z_]\\w*'
name_wide: r'[^),]*'

comment: '--' any
```
