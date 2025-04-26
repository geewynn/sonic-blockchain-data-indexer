def hex_to_int(hex_value: str):
    if hex_value is None:
        return None
    if isinstance(hex_value, int):
        return hex_value
    return int(hex_value, 16) if hex_value.startswith("0x") else int(hex_value, 16)
