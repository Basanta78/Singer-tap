from setuptools import setup

setup(
    name="tap-student",
    version="0.1.0",
    description="Singer.io tap for extracting data",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_student"],
    install_requires=[
        "singer-python>=5.0.12",
        "requests",
    ],
    entry_points="""
    [console_scripts]
    tap-student=tap_student:main
    """,
    packages=["tap_student"],
    package_data = {
        "schemas": ["tap_student/schemas/*.json"]
    },
    include_package_data=True,
)
