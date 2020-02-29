from setuptools import setup

setup(
    name='airflow_spark_k8s',
    version='0.1.4',
    author='Roi Teveth',
    author_email='roi.teveth@nielsen.com',
    packages=['airflow_spark_k8s',
              'airflow_spark_k8s.hooks',
              'airflow_spark_k8s.operators',
              'airflow_spark_k8s.sensors'],
    url='http://pypi.python.org/pypi/airflow-spark-k8s/',
    license='LICENSE.txt',
    description='Airflow integration for Spark On K8s',
    install_requires=[
        "apache-airflow >= 1.10.0",
        "kubernetes"
    ],
)
