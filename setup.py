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

        "pandas==2.3.0",
        "polars==1.31.0",
        "plotly==6.2.0",
        "numpy==2.3.1",
        "dask==2025.5.1",
        # "numba==1.26.0",
        "requests==2.32.3",
        "matplotlib==3.9.2",
        "nbconvert==7.16.6",
        "PyGetWindow==0.0.9",
        "nbformat==5.10.4",
        "tqdm==4.67.1",
        "streamlit==1.44.1",
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
