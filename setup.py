from setuptools import setup, find_packages

#Reference on setuptools usage
#https://pythonhosted.org/an_example_pypi_project/setuptools.html

setup(name='tubes',
      version='0.1',
      description='A library for moving data around on the internet',
      author='Bob Sherbert',
      author_email='bob@carbidelabs.com',
      packages=['tubes'],
      package_data={'tubes': ['*'],
                   },
      scripts = [],
      install_requires=['SQLAlchemy', 'pymongo'],
      zip_safe=True,
     )
