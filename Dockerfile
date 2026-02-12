FROM apache/spark:4.1.0-python3

USER root

# Install Python dependencies
COPY pyproject.toml /opt/benchmark/pyproject.toml
RUN pip install --no-cache-dir \
    "pyarrow>=17.0,<19.0" \
    "pandas>=2.0,<3.0" \
    "numpy>=1.26,<2.0" \
    "scipy>=1.12,<2.0" \
    "pytest>=8.0"

# Copy benchmark code
COPY src/ /opt/benchmark/src/
COPY tests/ /opt/benchmark/tests/
COPY results/ /opt/benchmark/results/

WORKDIR /opt/benchmark

ENV PYTHONPATH="/opt/benchmark/src:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

ENTRYPOINT ["python3", "-m", "spark_udf_benchmark"]
CMD ["--rows", "100000"]
