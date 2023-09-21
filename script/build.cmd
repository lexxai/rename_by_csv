python build-version.py

mkdir "../pyinstall"
rem ERASE "../pyinstall" /S/Q
PUSHD "../pyinstall"

pyinstaller "../txt2csv/main.py" --clean --name txt2csv --hidden-import=PIL --onefile --version-file "../versionfile.txt"
POPD
python build-version.py ../pyinstall/dist/txt2csv.exe