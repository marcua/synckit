import ez_setup
ez_setup.use_setuptools()

from setuptools import setup, find_packages
setup(
    name = "SyncKit",
    version = "0.2.1",
    packages = find_packages('python'),
    package_dir = {'':'python'},

    # We don't know enough about their install to actually require a django
    # download
    # install_requires = ['django>=1.1.1'],

    # metadata for upload to PyPI
    author = "Edward Benson and Adam Marcus",
    author_email = "synckit@csail.mit.edu",
    description = "Synchronize client-side HTML5 stores with a server-side database",
    license = "BSD",
    keywords = "HTML5 database client-side server-side",
    url = "http://github.com/synckit/synckit",
)

