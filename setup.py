from setuptools import setup, find_packages


requirements = [l.strip() for l in open('requirements.txt').readlines()]

setup(
    name="chash4py",
    version="1.0",
    packages=find_packages(),
    install_requires=requirements,
    include_package_data=True,

    # metadata for upload to PyPI
    author="Lin Hao",
    author_email="freej.cn@gmail.com",
    description="A simple implement of consistent hash (hash ring) by Python.",
    keywords="consistent hash ring",
    url="https://github.com/hyper-carrot/chash4py",
    platforms = "Independant",
)
