from setuptools import setup, find_packages

setup(
    name="pgc_backtest",
    version="0.1.0",
    description="A Python package for backtesting trading strategies",
    long_description="pgc_backtest is a Python module designed for backtesting trading strategies using pandas, numpy, and other essential libraries.",
    long_description_content_type="text/plain",
    author="Vikas Sharma",
    author_email="Jnv2252@Gmail.com",
    license="MIT",
    packages=find_packages(),
    install_requires=[
        "pandas==2.3.1",
        "polars==1.27.1",
        "plotly==6.2.0",
        "numpy==2.2.6",
        "dask==2025.7.0",
        "numba==0.61.2",
        "streamlit==1.44.1",
        "requests",
        "tqdm",
    ],
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Financial and Trading",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Office/Business :: Financial :: Investment",
    ],
)
