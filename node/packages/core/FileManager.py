from flask import request
from pathlib import Path
import zipfile
import shutil
import tempfile
import requests


PACKAGE_TARGETS = {
    "routines": Path("./packages/routines"),
    "services": Path("./packages/services"),
}



def safe_extract_zip(zip_path: Path, extract_to: Path):
    extract_to = extract_to.resolve()

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        for member in zip_ref.namelist():
            target_path = (extract_to / member).resolve()

            if not str(target_path).startswith(str(extract_to)):
                raise ValueError(f"Unsafe zip path: {member}")

        zip_ref.extractall(extract_to)


def sync_package(secret):
    auth = request.headers.get("Authorization")

    if auth != f"Bearer {secret}":
        return {"status": "error", "error": "unauthorized"}

    package_type = request.form.get("package_type")

    if package_type not in PACKAGE_TARGETS:
        return {
            "status": "error",
            "error": "invalid package_type"
        }

    if "bundle" not in request.files:
        return {
            "status": "error",
            "error": "missing bundle"
        }

    target_dir = PACKAGE_TARGETS[package_type]
    print(target_dir)
    target_dir.parent.mkdir(parents=True, exist_ok=True)

    uploaded_file = request.files["bundle"]

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        zip_path = tmp_path / "bundle.zip"
        extract_path = tmp_path / "extracted"

        extract_path.mkdir()
        uploaded_file.save(zip_path)

        try:
            safe_extract_zip(zip_path, extract_path)
        except Exception as e:
            return {
                "status": "error",
                "error": f"invalid zip: {e}"
            }

        # The zip should contain either:
        # routines/... or services/...
        new_package_path = extract_path / package_type

        if not new_package_path.exists():
            return {
                "status": "error",
                "error": f"zip does not contain {package_type} folder"
            }

        backup_dir = target_dir.with_name(target_dir.name + "_backup")

        if backup_dir.exists():
            shutil.rmtree(backup_dir)

        if target_dir.exists():
            target_dir.rename(backup_dir)

        shutil.move(str(new_package_path), str(target_dir))

    return {
        "status": "ok",
        "package_type": package_type,
        "message": f"{package_type} synced"
    }


def send_package_to_node(node_secret, node_url: str, package_type: str, folder_path: str):
    folder_path = Path(folder_path)

    if package_type not in ["routines", "services"]:
        raise ValueError("package_type must be 'routines' or 'services'")

    if not folder_path.exists():
        raise FileNotFoundError(f"Folder does not exist: {folder_path}")

    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)

        zip_base = tmp_path / folder_path.name

        zip_file = shutil.make_archive(
            base_name=str(zip_base),
            format="zip",
            root_dir=folder_path.parent,
            base_dir=folder_path.name
        )

        with open(zip_file, "rb") as f:
            response = requests.post(
                f"{node_url}/sync/package",
                headers={
                    "Authorization": f"Bearer {node_secret}"
                },
                data={
                    "package_type": package_type
                },
                files={
                    "bundle": (f"{package_type}.zip", f, "application/zip")
                },
                timeout=10
            )

    response.raise_for_status()
    return response.json()
