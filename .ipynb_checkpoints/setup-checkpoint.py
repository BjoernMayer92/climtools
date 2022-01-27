from setuptools import setup

setup(name='climtools',
      version='0.1',
      description='Tools for preprocessing climate data',
      url='http://github.com/bjoern.mayer92/climtools',
      author='Björn Mayer',
      author_email='bjoern.mayer92@gmail.com',
      license='MIT',
      packages=['climtools'],      
      install_requires=['xarray'],
      zip_safe=False)