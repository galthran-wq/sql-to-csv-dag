def split_ignoring_strings(string, sep=",", str_chars=["'", '"']):
    """
    Splits a string by a separator, ignoring string-like values.
    Supposed to be used for mysql dump files.
    Example input:
    ("value1", 3, 'value2', 4, "Complicated, value")
    Example output:
    ["value1", "3", "value2", "4", "Complicated, value"]
    """
    occurence_indices = []
    is_inside_str_chars = [ 0 for str_char in str_chars ]
    for i, char in enumerate(string):

        try:
            char_index = str_chars.index(char)
        except ValueError:
            char_index = -1

        if char_index != -1:
            is_inside_str_chars[char_index] += 1
        if char == sep and not any([el % 2 != 0 for el in is_inside_str_chars]):
            occurence_indices.append(i)
    if len(occurence_indices) == 0:
        return [string]
    parts = []
    last_index = -1
    for index in occurence_indices:
        parts.append(string[last_index+1:index])
        last_index = index
    if last_index+1 < len(string):
        parts.append(string[last_index+1:])
    return parts

