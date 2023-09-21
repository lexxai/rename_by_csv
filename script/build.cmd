python build-version.py

mkdir "../pyinstall"
ERASE "../pyinstall" /S/Q
PUSHD "../pyinstall"

pyinstaller "../rename_by_csv/main.py" --clean --name rename_by_csv --hidden-import=PIL --onefile --version-file "../versionfile.txt"
POPD
python build-version.py ../pyinstall/dist/rename_by_csv.exe