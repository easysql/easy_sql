SELECT a+b  AS foo,
c AS bar from my_table where name = {{ test_name }};    -- noqa: L014,L034
