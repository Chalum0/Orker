from pathlib import Path
import hashlib

def create_package_folders():
    folders = [
        Path("./packages/services"),
        Path("./packages/routines"),
    ]

    for folder in folders:
        folder.mkdir(parents=True, exist_ok=True)


def hash_folder(folder_path: str | Path) -> str:
    folder = Path(folder_path)
    hasher = hashlib.sha256()

    if not folder.exists():
        return ""

    for file_path in sorted(folder.rglob("*")):
        if file_path.is_file():
            relative_path = file_path.relative_to(folder).as_posix()

            # Include the file path in the hash
            hasher.update(relative_path.encode("utf-8"))
            hasher.update(b"\0")

            # Include the file content in the hash
            with file_path.open("rb") as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)

            hasher.update(b"\0")

    return hasher.hexdigest()



def hash_services_and_routines() -> dict:
    return {
        "services": hash_folder("./packages/services"),
        "routines": hash_folder("./packages/routines"),
    }
