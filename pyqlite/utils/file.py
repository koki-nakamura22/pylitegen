def save_as_text(save_path: str, text: str, encoding: str = 'utf-8') -> None:
    """
    Save text as a text file.

    Parameters
    ----------
    save_path: str
        The path of the file to save.
    text: str
        The data of the file to save.
    encoding: str, default 'utf-8'
        The file content encoding.
    ----------
    """
    with open(save_path, mode='w', encoding=encoding) as f:
        f.write(text)
