# Runs the quality-gate tests inside the streaming job image: same JVM
# (Java 17), same PySpark, same Python as production.
#
# We target the FILE and not the marker: `-m spark` would filter after
# collection, and collection imports every test module -- including those that
# depend on numpy or paho, which are absent from this image.
docker compose run --rm --no-deps -v "${PWD}:/work" -w /work `
  --entrypoint sh spark-streaming `
  -c "pip install --quiet pytest && SPARK_LOCAL_IP=127.0.0.1 python -m pytest tests/test_quality_gates.py"
