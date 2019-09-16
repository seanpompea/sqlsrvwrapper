from setuptools import setup, find_packages
setup(name='sqlsrvwrapper',
      version='0.0.0',
      packages=find_packages(),
      install_requires=['Cython', 'pymssql'],
      zip_safe=False)

