from setuptools import find_packages, setup

setup(
    name="mormon_queer_analysis",
    packages=find_packages(exclude=["mormon_queer_analysis_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud",
        "matplotlib",
        "openai",
        "pandas",
        "plotly",
        "requests",
        "scikit-learn",
        "scipy",
        "tenacity",
        "tiktoken",
    ],
    extras_require={"dev": ["black", "dagster-webserver", "pytest"]},
)
