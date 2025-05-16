PYTHON_VERSIONS=(
 "/opt/python/cp310-cp310/bin/python3.10"
 "/opt/python/cp311-cp311/bin/python3.11"
 "/opt/python/cp312-cp312/bin/python3.12"
)

#clean up inplace build
rm infinistore/*.so

rm -rf dist/ wheelhouse/
OLDPATH=$PATH
for PYTHON in "${PYTHON_VERSIONS[@]}"; do
    BINDIR="$(dirname $PYTHON)"
    export PATH="$BINDIR:$OLDPATH"

    pip wheel -v . --no-deps -w dist/
    #runtime will install ibverbs, so exclude it
    WHEEL_FILE=$(ls dist/*.whl)
    echo "WHEEL_FILE: ${WHEEL_FILE}"
    auditwheel repair dist/* --exclude libibverbs.so.1
    rm -rf dist/
done
