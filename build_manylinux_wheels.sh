PYTHON_VERSIONS=(
 "/opt/python/cp310-cp310/bin/python3.10"
 "/opt/python/cp311-cp311/bin/python3.11"
 "/opt/python/cp312-cp312/bin/python3.12"
)

rm -rf dist/ wheelhouse/
OLDPATH=$PATH
for PYTHON in "${PYTHON_VERSIONS[@]}"; do
    rm -rf build/
    BINDIR="$(dirname $PYTHON)"
    export PATH="$BINDIR:$OLDPATH"
    ${PYTHON} setup.py bdist_wheel
    #runtime will install ibverbs, so exclude it
    WHEEL_FILE=$(ls dist/*.whl)
    echo "WHEEL_FILE: ${WHEEL_FILE}"
    auditwheel repair dist/* --exclude libibverbs.so.1
    rm -rf dist/
done
