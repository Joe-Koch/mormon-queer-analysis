from setuptools import find_packages, setup

setup(
    name="mormon_queer_analysis",
    packages=find_packages(exclude=["mormon_queer_analysis_tests"]),
    install_requires=["dagster", "dagster-cloud", "pandas", "matplotlib","requests"],
    extras_require={"dev": ["dagster-webserver", "pytest", "black"]},
)
