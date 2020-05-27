import setuptools

setuptools.setup(
    name='datasteward_df',
    version='0.0.1',
    install_requires=[],
    package_data={'': ['fields/*.json']},
    include_package_data=True,
    packages=setuptools.find_packages(),
)
