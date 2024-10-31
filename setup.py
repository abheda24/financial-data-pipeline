from setuptools import setup, find_packages

setup(
    name="financial_pipeline",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        'pandas',
        'yfinance',
        'psycopg2-binary',
        'sqlalchemy'
    ]
)