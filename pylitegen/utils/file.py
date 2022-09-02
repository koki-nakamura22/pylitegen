def save_as_text(save_path: str, text: str, encoding: str = 'utf-8') -> None:
    with open(save_path, mode='w', encoding=encoding) as f:
        f.write(text)
