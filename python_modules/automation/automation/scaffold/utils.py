import shutil


def copy_directory(src: str, dest: str) -> None:
    try:
        shutil.copytree(src, dest, ignore=shutil.ignore_patterns(".DS_Store"))
    except shutil.Error as e:
        print(f"Directory not copied. Error: {e}")
    except OSError as e:
        print(f"Directory not copied. Error: {e}")
