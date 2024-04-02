from pathlib import Path


def find_filepath(filename: str, include_home = False) -> Path:
    """
    Searchs the current user directory or current directory tree for requested filename
    """
    print('zz')
    print(Path.cwd())
    # Check current directory
    currentPath = Path(filename)
    if (currentPath.is_file()):
        return currentPath
    
    # Check Home
    currentPath = Path(Path.home() / filename)
    if (include_home and currentPath.is_file()):
        return currentPath

    
    # Check all parent directories for file
    all_paths = Path(__file__).resolve().parents
    for path in all_paths:
        print(path)
        currentPath = path / filename
        if (currentPath.is_file()):
            return currentPath
        

