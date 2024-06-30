from setuptools import find_packages, setup

setup(
    name="abstracts_gene_disease",
    packages=find_packages(exclude=["abstracts_gene_disease_tests"]),
    install_requires=[
        "dagster",
        "pandas",
        "polars",
#        "db-dtypes",
        'polars',
        'biomart'
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
