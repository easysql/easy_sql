def translate():
    return "CREATE FUNCTION IF NOT EXISTS translate AS (input, from, to) -> replaceAll(input, from, to)"
